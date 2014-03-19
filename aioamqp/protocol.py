"""
    Amqp Protocol
"""

import asyncio
import logging

import aioamqp
from . import channel as amqp_channel
from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions

logger = logging.getLogger(__name__)


class AmqpProtocol(asyncio.StreamReaderProtocol):

    def __init__(self, *args, **kwargs):
        super().__init__(asyncio.StreamReader(), self.client_connected)
        self.connecting = asyncio.Future()
        self.connection_closed = asyncio.Future()
        self.stop_now = asyncio.Future()
        self.is_connected = False
        self.server_version_major = None
        self.server_version_minor = None
        self.server_properties = None
        self.server_mechanisms = None
        self.server_locales = None

        self.channels = {}
        self.channels_max_id = 0

    def client_connected(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def connection_lost(self, exc):
        if not self.connection_closed.done():
            self.connection_closed.set_result(None)
        super().connection_lost(exc)

    @asyncio.coroutine
    def close_connection(self):
        if self.is_client:
            try:
                yield from asyncio.wait_for(self.connection_closed)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

            if self.state == 'CLOSED':
                return

    @asyncio.coroutine
    def start_connection(self, login="guest", password="guest", virtual_host="/", insist=False):
        """Initiate a connection at the protocol level
            We send `PROTOCOL_HEADER'
        """
        self.writer.write(amqp_constants.PROTOCOL_HEADER)

        # ensure we effectively have a response from the server
        yield from asyncio.wait_for(self.start(), timeout=amqp_constants.PROTOCOL_DEFAULT_TIMEOUT)

        client_properties = {
            'capabilities': {
                'consumer_cancel_notify': True,
                'connection.blocked': False,
            },
            'product_version': aioamqp.__version__,
            'product': aioamqp.__packagename__,
            'copyright': 'BSD',
        }
        auth = {
            'LOGIN': login,
            'PASSWORD': password,
        }

        yield from asyncio.wait_for(
            self.start_ok(client_properties, 'AMQPLAIN', auth, self.server_locales[0]),
            amqp_constants.PROTOCOL_DEFAULT_TIMEOUT)

        yield from asyncio.wait_for(
            self.tune(), amqp_constants.PROTOCOL_DEFAULT_TIMEOUT)

        tune_ok = {
            'channel_max': self.server_channel_max,
            'frame_max': self.server_frame_max,
            'heartbeat': self.hearbeat,
        }
        yield from asyncio.wait_for(
            self.tune_ok(**tune_ok), amqp_constants.PROTOCOL_DEFAULT_TIMEOUT)

        yield from asyncio.wait_for(
            self.open(virtual_host, capabilities='', insist=insist), amqp_constants.PROTOCOL_DEFAULT_TIMEOUT)

        yield from asyncio.wait_for(
            self.open_ok(), amqp_constants.PROTOCOL_DEFAULT_TIMEOUT)

        # for now, we read server's responses asynchronously
        self.worker = asyncio.async(self.run())

    @asyncio.coroutine
    def stop(self):
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
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE_OK): self.server_close,
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_START_OK): self.start_ok,
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
            yield from method_dispatch[(frame.class_id, frame.method_id)]
        except KeyError:
            logger.info("frame {} {} is not handled".format(frame.class_id, frame.method_id))

    @asyncio.coroutine
    def run(self):
        while not self.stop_now.done():
            try:
                yield from self.dispatch_frame()
            except Exception as exc:
                print(exc)

    @asyncio.coroutine
    def reply_to_hearbeat(self, frame):
        print("replyin' to heartbeat")
        frame = amqp_frame.AmqpRequest(self.writer, amqp_constants.TYPE_HEARTBEAT, 0)
        request = amqp_frame.AmqpEncoder()
        frame.write_frame(request)

    # Amqp specific methods
    @asyncio.coroutine
    def start(self):
        """Method sent from the server to begin a new connection"""
        frame = yield from self.get_frame()
        frame.frame()
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
        """The server is closing the connection

        """
        yield from self.client_close_ok()

        response = amqp_frame.AmqpDecoder(frame.payload)
        reply_code = response.read_short()
        reply_text = response.read_shortstr()
        #class_id = response.read_short()
        #method_id = response.read_short()
        raise exceptions.ClosedConnection("{} ({})".format(reply_text, reply_code))

    @asyncio.coroutine
    def server_close_ok(self, frame):
        """The server is accepting or connection close request

        """
        logger.debug("received close-ok frame")
        yield from self.stop()

    @asyncio.coroutine
    def client_close(self):
        """The client wants to close the connection

        """
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

    @asyncio.coroutine
    def client_close_ok(self):
        """The client ACK the connection close"""
        frame = amqp_frame.AmqpRequest(self.writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE_OK)

        yield from self.stop()

    @asyncio.coroutine
    def tune(self):
        frame = yield from self.get_frame()
        frame.frame()
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
        # TODO
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
    def open_ok(self):
        frame = yield from self.get_frame()
        frame.frame()

    @asyncio.coroutine
    def close(self):
        pass

    #
    ## aioamqp public methods
    #

    @asyncio.coroutine
    def channel(self):
        """Factory to create a new channel

        """
        self.channels_max_id += 1
        channel = amqp_channel.Channel(self, self.channels_max_id)
        self.channels[self.channels_max_id] = channel

        yield from channel.open()

        return channel
