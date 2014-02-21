"""
    Amqp Protocol
"""

import aioamqp
import asyncio

from . import channel as amqp_channel
from . import constants as amqp_constants
from . import frame as amqp_frame


class AmqpProtocol(asyncio.StreamReaderProtocol):

    def __init__(self, *args, **kwargs):
        super().__init__(asyncio.StreamReader(), self.client_connected)
        self.connection_closed = asyncio.Future()
        self.stop_now = asyncio.Future()
        self.is_connected = False
        self.server_version_major = None
        self.server_version_minor = None
        self.server_properties = None
        self.server_mechanisms = None
        self.server_locales = None

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
    def start_connection(self, virtual_host="/", insist=False):
        """Initiate a connection at the protocol level
            We send `PROTOCOL_HEADER'
        """
        self.writer.write(amqp_constants.PROTOCOL_HEADER)

        # ensure we effectively have a response from the server
        yield from asyncio.wait_for(self.start(), timeout=amqp_constants.PROTOCOL_DEFAULT_TIMEOUT)

        client_properties = {
            'capabilities': {
                'consumer_cancel_notify': True,
                'connection.blocked': True,
            },
            'product_version': aioamqp.__version__,
            'product': aioamqp.__packagename__,
            'copyright': 'BSD',
        }
        auth = {
            'LOGIN': 'guest',
            'PASSWORD': 'guest',
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
            self.open(virtual_host, insist), amqp_constants.PROTOCOL_DEFAULT_TIMEOUT)

        yield from asyncio.wait_for(
            self.open_ok(), amqp_constants.PROTOCOL_DEFAULT_TIMEOUT)

    @asyncio.coroutine
    def stop(self):
        self.stop_now.set_result(None)

    @asyncio.coroutine
    def run(self):
        """

        """
        while not self.connection_closed.done():
            frame_task = asyncio.Task(self.get_frame())
            pending, done = asyncio.wait([frame_task, self.connection_closed])
            print(pending, done)

    @asyncio.coroutine
    def get_frame(self):
        """
            Read a frame, without decoding it
        """
        frame = amqp_frame.AmqpResponse(self.reader)
        yield from frame.read_frame()

        return frame

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