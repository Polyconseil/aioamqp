"""
    Amqp Protocol
"""

import asyncio
import logging

from . import channel as amqp_channel
from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions
from . import version


logger = logging.getLogger(__name__)


class AmqpProtocol(asyncio.StreamReaderProtocol):
    """The AMQP protocol for asyncio.

    See http://docs.python.org/3.4/library/asyncio-protocol.html#protocols for more information
    on asyncio's protocol API.
    """

    CHANNEL_FACTORY = amqp_channel.Channel

    def __init__(self, *args, **kwargs):
        super().__init__(asyncio.StreamReader(), self.client_connected)
        self.connecting = asyncio.Future()
        self.connection_closed = asyncio.Event()
        self.stop_now = asyncio.Future()
        self.is_open = False
        self.version_major = None
        self.version_minor = None
        self.server_properties = None
        self.server_mechanisms = None
        self.server_locales = None
        self.reader = None
        self.writer = None
        self.worker = None
        self.hearbeat = None
        self.channels = {}
        self.server_frame_max = None
        self.server_channel_max = None
        self.channels_max_id = 0

    def client_connected(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def connection_lost(self, exc):
        logger.warning("Connection lost exc=%r", exc)
        self.connection_closed.set()
        self.is_open = False
        super().connection_lost(exc)

    @asyncio.coroutine
    def close(self, no_wait=False, timeout=None):
        """Close connection (and all channels)"""
        frame = amqp_frame.AmqpRequest(self.writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE)
        encoder = amqp_frame.AmqpEncoder(frame.payload)
        # we request a clean connection close
        encoder.write_short(0)
        encoder.write_shortstr('')
        encoder.write_short(0)
        encoder.write_short(0)
        frame.write_frame(encoder)
        if not no_wait:
            yield from self.wait_closed(timeout=timeout)

    @asyncio.coroutine
    def wait_closed(self, timeout=None):
        yield from asyncio.wait_for(self.connection_closed.wait(), timeout=timeout)

    @asyncio.coroutine
    def close_ok(self, frame):
        self.stop()
        frame.frame()
        logger.info("Recv close ok")

    @asyncio.coroutine
    def start_connection(self, host, port, login, password, virtualhost, ssl=False,
            login_method='AMQPLAIN', insist=False):
        """Initiate a connection at the protocol level
            We send `PROTOCOL_HEADER'
        """

        if ssl:
            # TODO
            logger.warning('ssl is not supported yet, falling back to non-secure connection')

        if login_method != 'AMQPLAIN':
            # TODO
            logger.warning('only AMQPLAIN login_method is supported, falling back to AMQPLAIN')

        self.writer.write(amqp_constants.PROTOCOL_HEADER)

        # Wait 'start' method from the server
        yield from self.dispatch_frame()

        client_properties = {
            'capabilities': {
                'consumer_cancel_notify': True,
                'connection.blocked': False,
            },
            'product_version': version.__version__,
            'product': version.__packagename__,
            'copyright': 'BSD',
        }
        auth = {
            'LOGIN': login,
            'PASSWORD': password,
        }

        # waiting reply start with credentions and co
        yield from self.start_ok(client_properties, 'AMQPLAIN', auth, self.server_locales[0])

        # wait for a "tune" reponse
        yield from self.dispatch_frame()

        tune_ok = {
            'channel_max': self.server_channel_max,
            'frame_max': self.server_frame_max,
            'heartbeat': self.hearbeat,
        }
        # "tune" the connexion with max channel, max frame, heartbeat
        yield from self.tune_ok(**tune_ok)

        # open a virtualhost
        yield from self.open(virtualhost, capabilities='', insist=insist)

        # wait for open-ok
        yield from self.dispatch_frame()

        # for now, we read server's responses asynchronously
        self.worker = asyncio.async(self.run())

    def stop(self):
        self.is_open = False
        self.connection_closed.set()
        self.stop_now.set_result(None)

    @asyncio.coroutine
    def get_frame(self):
        """Read the frame, and only decode its header

        """
        frame = amqp_frame.AmqpResponse(self.reader)
        yield from frame.read_frame()
        return frame

    @asyncio.coroutine
    def dispatch_frame(self, frame=None):
        """Dispatch the received frame to the corresponding handler"""

        method_dispatch = {
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE): self.server_close,
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE_OK): self.close_ok,
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_TUNE): self.tune,
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_START): self.start,
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_OPEN_OK): self.open_ok,
        }
        if not frame:
            frame = yield from self.get_frame()
            #print("frame.channel {} class_id {}".format(frame.channel, frame.class_id))

        if frame.frame_type == amqp_constants.TYPE_HEARTBEAT:
            yield from self.reply_to_hearbeat(frame)
            return

        if frame.channel is not 0:
            try:
                yield from self.channels[frame.channel].dispatch_frame(frame)
            except KeyError:
                logger.info("Unknown channel %s", frame.channel)
            return

        try:
            yield from method_dispatch[(frame.class_id, frame.method_id)](frame)
        except KeyError:
            logger.info("frame {} {} is not handled".format(frame.class_id, frame.method_id))

    @asyncio.coroutine
    def run(self):
        while not self.stop_now.done():
            try:
                yield from self.dispatch_frame()
            except exceptions.AmqpClosedConnection:
                logger.info("Close connection")
                self.stop_now.set_result(None)
            except Exception:
                logger.exception('error on dispatch')

    @asyncio.coroutine
    def reply_to_hearbeat(self, frame):
        logger.debug("replyin' to heartbeat")
        frame = amqp_frame.AmqpRequest(self.writer, amqp_constants.TYPE_HEARTBEAT, 0)
        request = amqp_frame.AmqpEncoder()
        frame.write_frame(request)

    # Amqp specific methods
    @asyncio.coroutine
    def start(self, frame):
        """Method sent from the server to begin a new connection"""
        response = amqp_frame.AmqpDecoder(frame.payload)

        self.version_major = response.read_octet()
        self.version_minor = response.read_octet()
        self.server_properties = response.read_table()
        self.server_mechanisms = response.read_longstr().split(' ')
        self.server_locales = response.read_longstr().split(' ')

    @asyncio.coroutine
    def start_ok(self, client_properties, mechanism, auth, locale):
        frame = amqp_frame.AmqpRequest(self.writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_START_OK)
        request = amqp_frame.AmqpEncoder()
        request.write_table(client_properties)
        request.write_shortstr(mechanism)
        request.write_table(auth)
        request.write_shortstr(locale.encode())
        frame.write_frame(request)

    @asyncio.coroutine
    def server_close(self, frame):
        """The server is closing the connection"""
        response = amqp_frame.AmqpDecoder(frame.payload)
        reply_code = response.read_short()
        reply_text = response.read_shortstr()
        class_id = response.read_short()
        method_id = response.read_short()
        self.stop()
        logger.warning("Server closed connection: %s, code=%s, class_id=%s, method_id=%s",
            reply_text, reply_code, class_id, method_id)

    @asyncio.coroutine
    def tune(self, frame):
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        self.server_channel_max = decoder.read_short()
        self.server_frame_max = decoder.read_long()
        self.hearbeat = decoder.read_short()

    @asyncio.coroutine
    def tune_ok(self, channel_max, frame_max, heartbeat):
        frame = amqp_frame.AmqpRequest(self.writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_TUNE_OK)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_short(channel_max)
        encoder.write_long(frame_max)
        encoder.write_short(heartbeat or 0)

        frame.write_frame(encoder)

    @asyncio.coroutine
    def secure_ok(self, login_response):
        pass

    @asyncio.coroutine
    def open(self, virtual_host, capabilities='', insist=False):
        """Open connection to virtual host."""
        frame = amqp_frame.AmqpRequest(self.writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_OPEN)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_shortstr(virtual_host)
        encoder.write_shortstr(capabilities)
        encoder.write_bool(insist)

        frame.write_frame(encoder)

    @asyncio.coroutine
    def open_ok(self, frame):
        self.is_open = True
        frame.frame()
        logger.info("Recv open ok")

    #
    ## aioamqp public methods
    #

    @asyncio.coroutine
    def channel(self):
        """Factory to create a new channel

        """
        self.channels_max_id += 1
        channel = self.CHANNEL_FACTORY(self, self.channels_max_id)
        self.channels[self.channels_max_id] = channel

        yield from channel.open()

        return channel
