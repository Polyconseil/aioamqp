"""
    Amqp channel specification
"""

import asyncio
import logging

from . import constants as amqp_constants
from . import frame as amqp_frame

logger = logging.getLogger(__name__)


class Channel:

    def __init__(self, protocol, channel_id):
        self.protocol = protocol
        self.channel_id = channel_id

        self.is_open = False

    @asyncio.coroutine
    def open(self):
        """Open the channel on the server"""
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_OPEN)
        request = amqp_frame.AmqpEncoder()
        request.write_shortstr('')
        frame.write_frame(request)

        yield from self.protocol.get_frame()

    @asyncio.coroutine
    def open_ok(self, frame):
        print(frame.frame())
        self.is_open = True

    @asyncio.coroutine
    def close(self):
        """Close the channel"""
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_CHANNEL, amqp_constants.CONNECTION_CLOSE)
        request = amqp_frame.AmqpEncoder()
        request.write_shortstr('')
        frame.write_frame(request)

    @asyncio.coroutine
    def close_ok(self, frame):
        self.is_open = False

    @asyncio.coroutine
    def dispatch_frame(self, frame):
        methods = {
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_OPEN_OK): self.open_ok,
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_CLOSE_OK): self.close_ok,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DECLARE_OK): self.exchange_delete_ok,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DELETE_OK): self.exchange_delete_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DECLARE_OK): self.queue_declare_ok,
        }

        yield from methods[(frame.class_id, frame.method_id)](frame)

    @asyncio.coroutine
    def exchange_declare(self, exchange_name, type_name, passive, durable,
                         auto_delete, internal, no_wait, arguments=None):
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DECLARE)
        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(exchange_name)
        request.write_shortstr(type_name)

        internal = False  # internal: deprecated

        request.write_bool(passive)
        request.write_bool(durable)
        request.write_bool(auto_delete)
        request.write_bool(internal)
        request.write_bool(no_wait)
        request.write_table(arguments)

        frame.write_frame(request)

    @asyncio.coroutine
    def exchange_declare_ok(self, frame):
        frame.frame()
        logger.debug("exchange declared")

    @asyncio.coroutine
    def exchange_delete(self, exchange_name, if_unused, no_wait):
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DECLARE)
        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(exchange_name)
        request.write_bool(if_unused)
        request.write_bool(no_wait)

        frame.write_frame(request)
        print("declare")

    @asyncio.coroutine
    def exchange_delete_ok(self, frame):
        frame.frame()
        logger.debug("exchange deleted")

    @asyncio.coroutine
    def queue_declare(self, queue_name, passive=False, durable=False,
                      exclusive=False, auto_delete=True, nowait=False, arguments=None):
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DECLARE)

        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(queue_name)
        request.write_bits(passive, durable, exclusive, auto_delete, nowait)
        request.write_bool(nowait)
        request.write_table(arguments)

        frame.write_frame(request)

    @asyncio.coroutine
    def queue_declare_ok(self, frame):
        frame.frame()
        logger.debug("queue declared")

#
## Public api
#

    @asyncio.coroutine
    def queue(self, queue_name):
        """Declare a queue on the server"""
        yield from self.queue_declare(queue_name)

    @asyncio.coroutine
    def exchange(self, exchange_name, type_name, passive=False, durable=False, arguments=None):
        """Declare an exchange on the server"""
        print("exchange_name", exchange_name)
        yield from self.exchange_declare(
            exchange_name, type_name, passive, durable,
            auto_delete=False, internal=False, no_wait=False)

    @asyncio.coroutine
    def queue_bind(self, queue_name, exchange_name, routing_key):
        """Bind a queue and a channel"""
        pass

    @asyncio.coroutine
    def basic_publish(self, message):
        """publish"""
        pass



