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
from .compat import ensure_future


logger = logging.getLogger(__name__)


class _StreamWriter(asyncio.StreamWriter):

    def write(self, data):
        ret = super().write(data)
        self._protocol._heartbeat_timer_send_reset()
        return ret

    def writelines(self, data):
        ret = super().writelines(data)
        self._protocol._heartbeat_timer_send_reset()
        return ret

    def write_eof(self):
        ret = super().write_eof()
        self._protocol._heartbeat_timer_send_reset()
        return ret


class AmqpProtocol(asyncio.StreamReaderProtocol):
    """The AMQP protocol for asyncio.

    See http://docs.python.org/3.4/library/asyncio-protocol.html#protocols for more information
    on asyncio's protocol API.

    """

    CHANNEL_FACTORY = amqp_channel.Channel

    def __init__(self, *args, **kwargs):
        """Defines our new protocol instance

        Args:
            channel_max: int, specifies highest channel number that the server permits.
                              Usable channel numbers are in the range 1..channel-max.
                              Zero indicates no specified limit.
            frame_max: int, the largest frame size that the server proposes for the connection,
                            including frame header and end-byte. The client can negotiate a lower value.
                            Zero means that the server does not impose any specific limit
                            but may reject very large frames if it cannot allocate resources for them.
            heartbeat: int, the delay, in seconds, of the connection heartbeat that the server wants.
                            Zero means the server does not want a heartbeat.
            loop: Asyncio.Eventloop: specify the eventloop to use.
            client_properties: dict, client-props to tune the client identification
        """
        self._loop = kwargs.get('loop') or asyncio.get_event_loop()
        super().__init__(asyncio.StreamReader(loop=self._loop), loop=self._loop)
        self._on_error_callback = kwargs.get('on_error')

        self.client_properties = kwargs.get('client_properties', {})
        self.connection_tunning = {}
        if 'channel_max' in kwargs:
            self.connection_tunning['channel_max'] = kwargs.get('channel_max')
        if 'frame_max' in kwargs:
            self.connection_tunning['frame_max'] = kwargs.get('frame_max')
        if 'heartbeat' in kwargs:
            self.connection_tunning['heartbeat'] = kwargs.get('heartbeat')

        self.connecting = asyncio.Future(loop=self._loop)
        self.connection_closed = asyncio.Event(loop=self._loop)
        self.stop_now = asyncio.Future(loop=self._loop)
        self.is_open = False
        self.version_major = None
        self.version_minor = None
        self.server_properties = None
        self.server_mechanisms = None
        self.server_locales = None
        self.worker = None
        self.server_heartbeat = None
        self._heartbeat_timer_recv = None
        self._heartbeat_timer_send = None
        self.channels = {}
        self.server_frame_max = None
        self.server_channel_max = None
        self.channels_ids_ceil = 0
        self.channels_ids_free = set()

    def connection_made(self, transport):
        super().connection_made(transport)
        self._stream_writer = _StreamWriter(transport, self, self._stream_reader, self._loop)

    def connection_lost(self, exc):
        logger.warning("Connection lost exc=%r", exc)
        self.connection_closed.set()
        self.is_open = False
        self._close_channels(exception=exc)
        self._heartbeat_stop()
        super().connection_lost(exc)

    def data_received(self, data):
        self._heartbeat_timer_recv_reset()
        super().data_received(data)

    @asyncio.coroutine
    def close(self, no_wait=False, timeout=None):
        """Close connection (and all channels)"""
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_METHOD, 0)
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
        yield from asyncio.wait_for(self.connection_closed.wait(), timeout=timeout, loop=self._loop)

    @asyncio.coroutine
    def close_ok(self, frame):
        logger.info("Recv close ok")
        self._stream_writer.close()

    @asyncio.coroutine
    def start_connection(self, host, port, login, password, virtualhost, ssl=False,
            login_method='AMQPLAIN', insist=False):
        """Initiate a connection at the protocol level
            We send `PROTOCOL_HEADER'
        """

        if login_method != 'AMQPLAIN':
            # TODO
            logger.warning('only AMQPLAIN login_method is supported, falling back to AMQPLAIN')

        self._stream_writer.write(amqp_constants.PROTOCOL_HEADER)

        # Wait 'start' method from the server
        yield from self.dispatch_frame()

        client_properties = {
            'capabilities': {
                'consumer_cancel_notify': True,
                'connection.blocked': False,
            },
            'copyright': 'BSD',
            'product': version.__package__,
            'product_version': version.__version__,
        }
        client_properties.update(self.client_properties)
        auth = {
            'LOGIN': login,
            'PASSWORD': password,
        }

        # waiting reply start with credentions and co
        yield from self.start_ok(client_properties, 'AMQPLAIN', auth, self.server_locales[0])

        # wait for a "tune" reponse
        yield from self.dispatch_frame()

        tune_ok = {
            'channel_max': self.connection_tunning.get('channel_max', self.server_channel_max),
            'frame_max': self.connection_tunning.get('frame_max', self.server_frame_max),
            'heartbeat': self.connection_tunning.get('heartbeat', self.server_heartbeat),
        }
        # "tune" the connexion with max channel, max frame, heartbeat
        yield from self.tune_ok(**tune_ok)

        # update connection tunning values
        self.server_frame_max = tune_ok['frame_max']
        self.server_channel_max = tune_ok['channel_max']
        self.server_heartbeat = tune_ok['heartbeat']

        if self.server_heartbeat > 0:
            self._heartbeat_timer_recv_reset()
            self._heartbeat_timer_send_reset()

        # open a virtualhost
        yield from self.open(virtualhost, capabilities='', insist=insist)

        # wait for open-ok
        frame = yield from self.get_frame()
        yield from self.dispatch_frame(frame)
        if (frame.frame_type == amqp_constants.TYPE_METHOD and
                frame.class_id == amqp_constants.CLASS_CONNECTION and
                frame.method_id == amqp_constants.CONNECTION_CLOSE):
            raise exceptions.AmqpClosedConnection()

        # for now, we read server's responses asynchronously
        self.worker = ensure_future(self.run(), loop=self._loop)

    @asyncio.coroutine
    def get_frame(self):
        """Read the frame, and only decode its header

        """
        frame = amqp_frame.AmqpResponse(self._stream_reader)
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

        if frame.frame_type == amqp_constants.TYPE_HEARTBEAT:
            return

        if frame.channel is not 0:
            channel = self.channels.get(frame.channel)
            if channel is not None:
                yield from channel.dispatch_frame(frame)
            else:
                logger.info("Unknown channel %s", frame.channel)
            return

        if (frame.class_id, frame.method_id) not in method_dispatch:
            logger.info("frame %s %s is not handled", frame.class_id, frame.method_id)
            return
        yield from method_dispatch[(frame.class_id, frame.method_id)](frame)

    def release_channel_id(self, channel_id):
        """Called from the channel instance, it relase a previously used
        channel_id
        """
        self.channels_ids_free.add(channel_id)

    @property
    def channels_ids_count(self):
        return self.channels_ids_ceil - len(self.channels_ids_free)

    def _close_channels(self, reply_code=None, reply_text=None, exception=None):
        """Cleanly close channels

            Args:
                reply_code:     int, the amqp error code
                reply_text:     str, the text associated to the error_code
                exc:            the exception responsible of this error

        """
        if exception is None:
            exception = exceptions.ChannelClosed(reply_code, reply_text)

        if self._on_error_callback:
            if asyncio.iscoroutinefunction(self._on_error_callback):
                ensure_future(self._on_error_callback(exception), loop=self._loop)
            else:
                self._on_error_callback(exceptions.ChannelClosed(exception))

        for channel in self.channels.values():
            channel.connection_closed(reply_code, reply_text, exception)

    @asyncio.coroutine
    def run(self):
        while not self.stop_now.done():
            try:
                yield from self.dispatch_frame()
            except exceptions.AmqpClosedConnection as exc:
                logger.info("Close connection")
                self.stop_now.set_result(None)

                self._close_channels(exception=exc)
            except Exception:  # pylint: disable=broad-except
                logger.exception('error on dispatch')

    @asyncio.coroutine
    def heartbeat(self):
        """ deprecated heartbeat coroutine

        This coroutine is now a no-op as the heartbeat is handled directly by
        the rest of the AmqpProtocol class.  This is kept around for backwards
        compatibility purposes only.
        """
        yield from self.stop_now

    def send_heartbeat(self):
        """Sends an heartbeat message.
        It can be an ack for the server or the client willing to check for the
        connexion timeout
        """
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_HEARTBEAT, 0)
        request = amqp_frame.AmqpEncoder()
        frame.write_frame(request)

    def _heartbeat_timer_recv_timeout(self):
        # 4.2.7 If a peer detects no incoming traffic (i.e. received octets) for
        # two heartbeat intervals or longer, it should close the connection
        # without following the Connection.Close/Close-Ok handshaking, and log
        # an error.
        # TODO(rcardona) raise a "timeout" exception somewhere
        self._stream_writer.close()

    def _heartbeat_timer_recv_reset(self):
        if self.server_heartbeat is None:
            return
        if self._heartbeat_timer_recv is not None:
            self._heartbeat_timer_recv.cancel()
        self._heartbeat_timer_recv = self._loop.call_later(
            self.server_heartbeat * 2,
            self._heartbeat_timer_recv_timeout)

    def _heartbeat_timer_send_reset(self):
        if self.server_heartbeat is None:
            return
        if self._heartbeat_timer_send is not None:
            self._heartbeat_timer_send.cancel()
        self._heartbeat_timer_send = self._loop.call_later(
            self.server_heartbeat,
            self.send_heartbeat)

    def _heartbeat_stop(self):
        self.server_heartbeat = None
        if self._heartbeat_timer_recv is not None:
            self._heartbeat_timer_recv.cancel()
        if self._heartbeat_timer_send is not None:
            self._heartbeat_timer_send.cancel()

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
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_METHOD, 0)
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
        logger.warning("Server closed connection: %s, code=%s, class_id=%s, method_id=%s",
            reply_text, reply_code, class_id, method_id)
        self._close_channels(reply_code, reply_text)
        self._close_ok()
        self._stream_writer.close()

    def _close_ok(self):
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE_OK)
        request = amqp_frame.AmqpEncoder()
        frame.write_frame(request)

    @asyncio.coroutine
    def tune(self, frame):
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        self.server_channel_max = decoder.read_short()
        self.server_frame_max = decoder.read_long()
        self.server_heartbeat = decoder.read_short()

    @asyncio.coroutine
    def tune_ok(self, channel_max, frame_max, heartbeat):
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_TUNE_OK)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_short(channel_max)
        encoder.write_long(frame_max)
        encoder.write_short(heartbeat)

        frame.write_frame(encoder)

    @asyncio.coroutine
    def secure_ok(self, login_response):
        pass

    @asyncio.coroutine
    def open(self, virtual_host, capabilities='', insist=False):
        """Open connection to virtual host."""
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_METHOD, 0)
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
        logger.info("Recv open ok")

    #
    ## aioamqp public methods
    #

    @asyncio.coroutine
    def channel(self, **kwargs):
        """Factory to create a new channel

        """
        try:
            channel_id = self.channels_ids_free.pop()
        except KeyError:
            assert self.server_channel_max is not None, 'connection channel-max tuning not performed'
            # channel-max = 0 means no limit
            if self.server_channel_max and self.channels_ids_ceil > self.server_channel_max:
                raise exceptions.NoChannelAvailable()
            self.channels_ids_ceil += 1
            channel_id = self.channels_ids_ceil
        channel = self.CHANNEL_FACTORY(self, channel_id, **kwargs)
        self.channels[channel_id] = channel
        yield from channel.open()
        return channel
