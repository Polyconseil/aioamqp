"""
    Amqp Protocol
"""

import asyncio
import logging

import pamqp.commands
import pamqp.frame
import pamqp.heartbeat

from . import channel as amqp_channel
from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions
from . import version


logger = logging.getLogger(__name__)


CONNECTING, OPEN, CLOSING, CLOSED = range(4)


class _StreamWriter(asyncio.StreamWriter):

    def write(self, data):
        super().write(data)
        self._protocol._heartbeat_timer_send_reset()

    def writelines(self, data):
        super().writelines(data)
        self._protocol._heartbeat_timer_send_reset()

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
            client_properties: dict, client-props to tune the client identification
        """
        self._reader = asyncio.StreamReader()
        super().__init__(self._reader)
        self._on_error_callback = kwargs.get('on_error')

        self.client_properties = kwargs.get('client_properties', {})
        self.connection_tunning = {}
        if 'channel_max' in kwargs:
            self.connection_tunning['channel_max'] = kwargs.get('channel_max')
        if 'frame_max' in kwargs:
            self.connection_tunning['frame_max'] = kwargs.get('frame_max')
        if 'heartbeat' in kwargs:
            self.connection_tunning['heartbeat'] = kwargs.get('heartbeat')

        self.connecting = asyncio.Future()
        self.connection_closed = asyncio.Event()
        self.stop_now = asyncio.Event()
        self.state = CONNECTING
        self.version_major = None
        self.version_minor = None
        self.server_properties = None
        self.server_mechanisms = None
        self.server_locales = None
        self.worker = None
        self.server_heartbeat = None
        self._heartbeat_last_recv = None
        self._heartbeat_last_send = None
        self._heartbeat_recv_worker = None
        self._heartbeat_send_worker = None
        self.channels = {}
        self.server_frame_max = None
        self.server_channel_max = None
        self.channels_ids_ceil = 0
        self.channels_ids_free = set()
        self._drain_lock = asyncio.Lock()

    def connection_made(self, transport):
        super().connection_made(transport)
        self._stream_writer = _StreamWriter(transport, self, self._stream_reader,
                                            loop=asyncio.get_running_loop())

    def eof_received(self):
        super().eof_received()
        # Python 3.5+ started returning True here to keep the transport open.
        # We really couldn't care less so keep the behavior from 3.4 to make
        # sure connection_lost() is called.
        return False

    def connection_lost(self, exc):
        if exc is not None:
            logger.warning("Connection lost exc=%r", exc)
        self.connection_closed.set()
        self.state = CLOSED
        self._close_channels(exception=exc)
        self._heartbeat_stop()
        super().connection_lost(exc)

    def data_received(self, data):
        self._heartbeat_timer_recv_reset()
        super().data_received(data)

    async def ensure_open(self):
        # Raise a suitable exception if the connection isn't open.
        # Handle cases from the most common to the least common.

        if self.state == OPEN:
            return

        if self.state == CLOSED:
            raise exceptions.AmqpClosedConnection()

        # If the closing handshake is in progress, let it complete.
        if self.state == CLOSING:
            await self.wait_closed()
            raise exceptions.AmqpClosedConnection()

        # Control may only reach this point in buggy third-party subclasses.
        assert self.state == CONNECTING
        raise exceptions.AioamqpException("connection isn't established yet.")

    async def _drain(self):
        async with self._drain_lock:
            # drain() cannot be called concurrently by multiple coroutines:
            # http://bugs.python.org/issue29930. Remove this lock when no
            # version of Python where this bugs exists is supported anymore.
            await self._stream_writer.drain()

    async def _write_frame(self, channel_id, request, drain=True):
        amqp_frame.write(self._stream_writer, channel_id, request)
        if drain:
            await self._drain()

    async def close(self, no_wait=False, timeout=None):
        """Close connection (and all channels)"""
        await self.ensure_open()
        self.state = CLOSING
        request = pamqp.commands.Connection.Close(
            reply_code=0,
            reply_text='',
            class_id=0,
            method_id=0
        )

        await self._write_frame(0, request)
        if not no_wait:
            await self.wait_closed(timeout=timeout)

    async def wait_closed(self, timeout=None):
        await asyncio.wait_for(self.connection_closed.wait(), timeout=timeout)
        if self._heartbeat_send_worker is not None:
            try:
                await asyncio.wait_for(self._heartbeat_send_worker, timeout=timeout)
            except asyncio.CancelledError:
                pass

    async def start_connection(self, host, port, login, password, virtualhost, ssl=False,
            login_method='PLAIN', insist=False):
        """Initiate a connection at the protocol level
            We send `PROTOCOL_HEADER'
        """

        if login_method != 'PLAIN':
            logger.warning('login_method %s is not supported, falling back to PLAIN', login_method)

        self._stream_writer.write(amqp_constants.PROTOCOL_HEADER)

        # Wait 'start' method from the server
        await self.dispatch_frame()

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
        await self.start_ok(client_properties, 'PLAIN', auth, self.server_locales)

        # wait for a "tune" reponse
        await self.dispatch_frame()

        tune_ok = {
            'channel_max': self.connection_tunning.get('channel_max', self.server_channel_max),
            'frame_max': self.connection_tunning.get('frame_max', self.server_frame_max),
            'heartbeat': self.connection_tunning.get('heartbeat', self.server_heartbeat),
        }
        # "tune" the connexion with max channel, max frame, heartbeat
        await self.tune_ok(**tune_ok)

        # update connection tunning values
        self.server_frame_max = tune_ok['frame_max']
        self.server_channel_max = tune_ok['channel_max']
        self.server_heartbeat = tune_ok['heartbeat']

        if self.server_heartbeat > 0:
            self._heartbeat_timer_recv_reset()
            self._heartbeat_timer_send_reset()

        # open a virtualhost
        await self.open(virtualhost, capabilities='', insist=insist)

        # wait for open-ok
        channel, frame = await self.get_frame()
        await self.dispatch_frame(channel, frame)

        await self.ensure_open()
        # for now, we read server's responses asynchronously
        self.worker = asyncio.ensure_future(self.run())

    async def get_frame(self):
        """Read the frame, and only decode its header

        """
        return await amqp_frame.read(self._stream_reader)

    async def dispatch_frame(self, frame_channel=None, frame=None):
        """Dispatch the received frame to the corresponding handler"""

        method_dispatch = {
            pamqp.commands.Connection.Close.name: self.server_close,
            pamqp.commands.Connection.CloseOk.name: self.close_ok,
            pamqp.commands.Connection.Tune.name: self.tune,
            pamqp.commands.Connection.Start.name: self.start,
            pamqp.commands.Connection.OpenOk.name: self.open_ok,
        }
        if frame_channel is None and frame is None:
            frame_channel, frame = await self.get_frame()

        if isinstance(frame, pamqp.heartbeat.Heartbeat):
            return

        if frame_channel != 0:
            channel = self.channels.get(frame_channel)
            if channel is not None:
                await channel.dispatch_frame(frame)
            else:
                logger.info("Unknown channel %s", frame_channel)
            return

        if frame.name not in method_dispatch:
            logger.info("frame %s is not handled", frame.name)
            return
        await method_dispatch[frame.name](frame)

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
                asyncio.ensure_future(self._on_error_callback(exception))
            else:
                self._on_error_callback(exceptions.ChannelClosed(exception))

        for channel in self.channels.values():
            channel.connection_closed(reply_code, reply_text, exception)

    async def run(self):
        while not self.stop_now.is_set():
            try:
                await self.dispatch_frame()
            except exceptions.AmqpClosedConnection as exc:
                logger.info("Close connection")
                self.stop_now.set()

                self._close_channels(exception=exc)
            except Exception:  # pylint: disable=broad-except
                logger.exception('error on dispatch')

    async def heartbeat(self):
        """ deprecated heartbeat coroutine

        This coroutine is now a no-op as the heartbeat is handled directly by
        the rest of the AmqpProtocol class.  This is kept around for backwards
        compatibility purposes only.
        """
        await self.stop_now.wait()

    async def send_heartbeat(self):
        """Sends an heartbeat message.
        It can be an ack for the server or the client willing to check for the
        connexion timeout
        """
        request = pamqp.heartbeat.Heartbeat()
        await self._write_frame(0, request)

    def _heartbeat_timer_recv_reset(self):
        if self.server_heartbeat is None or self.server_heartbeat == 0:
            return
        self._heartbeat_last_recv = asyncio.get_running_loop().time()
        if self._heartbeat_recv_worker is None:
            self._heartbeat_recv_worker = asyncio.ensure_future(self._heartbeat_recv())

    def _heartbeat_timer_send_reset(self):
        if self.server_heartbeat is None or self.server_heartbeat == 0:
            return
        self._heartbeat_last_send = asyncio.get_running_loop().time()
        if self._heartbeat_send_worker is None:
            self._heartbeat_send_worker = asyncio.ensure_future(self._heartbeat_send())

    def _heartbeat_stop(self):
        self.server_heartbeat = None
        if self._heartbeat_recv_worker is not None:
            self._heartbeat_recv_worker.cancel()
        if self._heartbeat_send_worker is not None:
            self._heartbeat_send_worker.cancel()

    async def _heartbeat_recv(self):
        # 4.2.7 If a peer detects no incoming traffic (i.e. received octets) for
        # two heartbeat intervals or longer, it should close the connection
        # without following the Connection.Close/Close-Ok handshaking, and log
        # an error.
        # TODO(rcardona) raise a "timeout" exception somewhere
        now = asyncio.get_running_loop().time()
        time_since_last_recv = now - self._heartbeat_last_recv

        while self.state != CLOSED:
            sleep_for = self.server_heartbeat * 2 - time_since_last_recv
            await asyncio.sleep(sleep_for)

            now = asyncio.get_running_loop().time()
            time_since_last_recv = now - self._heartbeat_last_recv
            if time_since_last_recv >= self.server_heartbeat * 2:
                self._stream_writer.close()

    async def _heartbeat_send(self):
        now = asyncio.get_running_loop().time()
        time_since_last_send = now - self._heartbeat_last_send

        while self.state != CLOSED:
            sleep_for = self.server_heartbeat - time_since_last_send
            await asyncio.sleep(sleep_for)

            now = asyncio.get_running_loop().time()
            time_since_last_send = now - self._heartbeat_last_send
            if time_since_last_send >= self.server_heartbeat:
                await self.send_heartbeat()
                time_since_last_send = now - self._heartbeat_last_send

    # Amqp specific methods
    async def start(self, frame):
        """Method sent from the server to begin a new connection"""
        self.version_major = frame.version_major
        self.version_minor = frame.version_minor
        self.server_properties = frame.server_properties
        self.server_mechanisms = frame.mechanisms
        self.server_locales = frame.locales

    async def start_ok(self, client_properties, mechanism, auth, locale):
        def credentials():
            return f'\0{auth["LOGIN"]}\0{auth["PASSWORD"]}'

        request = pamqp.commands.Connection.StartOk(
            client_properties=client_properties,
            mechanism=mechanism,
            locale=locale,
            response=credentials()
        )
        await self._write_frame(0, request)

    async def close_ok(self, frame):
        """In response to server close confirmation"""
        self.stop_now.set()
        self._stream_writer.close()

    async def server_close(self, frame):
        """The server is closing the connection"""
        self.state = CLOSING
        reply_code = frame.reply_code
        reply_text = frame.reply_text
        class_id = frame.class_id
        method_id = frame.method_id
        logger.warning("Server closed connection: %s, code=%s, class_id=%s, method_id=%s",
            reply_text, reply_code, class_id, method_id)
        self._close_channels(reply_code, reply_text)
        await self._close_ok()
        self.stop_now.set()
        self._stream_writer.close()

    async def _close_ok(self):
        """Send client close confirmation"""
        request = pamqp.commands.Connection.CloseOk()
        await self._write_frame(0, request)

    async def tune(self, frame):
        self.server_channel_max = frame.channel_max
        self.server_frame_max = frame.frame_max
        self.server_heartbeat = frame.heartbeat

    async def tune_ok(self, channel_max, frame_max, heartbeat):
        request = pamqp.commands.Connection.TuneOk(
            channel_max, frame_max, heartbeat
        )
        await self._write_frame(0, request)

    async def secure_ok(self, login_response):
        pass

    async def open(self, virtual_host, capabilities='', insist=False):
        """Open connection to virtual host."""
        request = pamqp.commands.Connection.Open(
            virtual_host, capabilities, insist
        )
        await self._write_frame(0, request)

    async def open_ok(self, frame):
        self.state = OPEN
        logger.info("Recv open ok")

    #
    ## aioamqp public methods
    #

    async def channel(self, **kwargs):
        """Factory to create a new channel

        """
        await self.ensure_open()
        try:
            channel_id = self.channels_ids_free.pop()
        except KeyError as ex:
            assert self.server_channel_max is not None, 'connection channel-max tuning not performed'
            # channel-max = 0 means no limit
            if self.server_channel_max and self.channels_ids_ceil > self.server_channel_max:
                raise exceptions.NoChannelAvailable() from ex
            self.channels_ids_ceil += 1
            channel_id = self.channels_ids_ceil
        channel = self.CHANNEL_FACTORY(self, channel_id, **kwargs)
        self.channels[channel_id] = channel
        await channel.open()
        return channel
