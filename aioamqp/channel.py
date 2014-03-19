"""
    Amqp channel specification
"""

import asyncio
import logging

from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions

logger = logging.getLogger(__name__)


class Channel:

    def __init__(self, protocol, channel_id):
        self.protocol = protocol
        self.channel_id = channel_id
        self.message_queue = None
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

    @asyncio.coroutine
    def open_ok(self, frame):
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
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_CLOSE): self.server_channel_close,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DECLARE_OK): self.exchange_delete_ok,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DELETE_OK): self.exchange_delete_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DECLARE_OK): self.queue_declare_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CONSUME_OK): self.basic_consume_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_DELIVER): self.basic_deliver,
        }
        yield from methods[(frame.class_id, frame.method_id)](frame)

    @asyncio.coroutine
    def exchange_declare(self, exchange_name, type_name, passive=False, durable=False,
                         auto_delete=False, internal=False, no_wait=False, arguments=None):
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

    @asyncio.coroutine
    def exchange_delete_ok(self, frame):
        frame.frame()
        logger.debug("exchange deleted")

    @asyncio.coroutine
    def queue_declare(self, queue_name, passive=False, durable=False,
                      exclusive=False, auto_delete=False, no_wait=False, arguments=None):
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DECLARE)

        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(queue_name)
        request.write_bits(passive, durable, exclusive, auto_delete, no_wait)
        request.write_table({})
        frame.write_frame(request)

    @asyncio.coroutine
    def queue_declare_ok(self, frame):
        frame.frame()
        logger.debug("queue declared")

    @asyncio.coroutine
    def server_channel_close(self, frame):
        frame.frame()
        response = amqp_frame.AmqpDecoder(frame.payload)
        reply_code = response.read_short()
        reply_text = response.read_shortstr()
        #class_id = response.read_short()
        #method_id = response.read_short()
        raise exceptions.ClosedConnection("{} ({})".format(reply_text, reply_code))

#
## Public api
#

    queue = queue_declare
    exchange = exchange_declare

    @asyncio.coroutine
    def queue_bind(self, queue_name, exchange_name, routing_key, no_wait=False, arguments=None):
        """Bind a queue and a channel"""
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_BIND)

        request = amqp_frame.AmqpEncoder()
        request.write_short(0)
        request.write_shortstr(queue_name)
        request.write_shortstr(exchange_name)
        request.write_shortstr(routing_key)
        request.write_octet(int(no_wait))
        request.write_table({})
        frame.write_frame(request)

    @asyncio.coroutine
    def exchange_bind(self, exchange_source, exchange_destination, routing_key, no_wait=False, arguments=None):
        """bind two exhanges together"""
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_BIND)
        request = amqp_frame.AmqpEncoder()
        request.write_short(0)
        request.write_shortstr(exchange_destination)
        request.write_shortstr(exchange_source)
        request.write_shortstr(routing_key)
        request.write_bit(no_wait)
        request.write_table({})
        frame.write_frame(request)

    @asyncio.coroutine
    def exchange_bind_ok(self, frame):
        logger.debug("exchange Bound")

    @asyncio.coroutine
    def publish(self, payload, exchange_name, routing_key, properties=None, mandatory=False, immediate=False):
        method_frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        method_frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_PUBLISH)

        method_request = amqp_frame.AmqpEncoder()
        method_request.write_short(0)
        method_request.write_shortstr(exchange_name)
        method_request.write_shortstr(routing_key)
        method_request.write_bits(mandatory, immediate)
        method_frame.write_frame(method_request)

        header_frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_HEADER, self.channel_id)
        header_frame.declare_class(amqp_constants.CLASS_BASIC)
        header_frame.set_body_size(len(payload))
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_message_properties(properties)

        header_frame.write_frame(encoder)

        content_frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_BODY, self.channel_id)
        content_frame.declare_class(amqp_constants.CLASS_BASIC)
        encoder = amqp_frame.AmqpEncoder()
        if isinstance(payload, str):
            encoder.payload.write(payload.encode())
        else:
            encoder.payload.write(payload)
        content_frame.write_frame(encoder)
        yield from self.protocol.writer.drain()
#
## Basic
#

    @asyncio.coroutine
    def basic_qos(self, prefetch_size, prefetch_count, connection_global):
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_QOS)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_long(prefetch_size)
        encoder.write_short(prefetch_count)
        encoder.write_bits(connection_global)
        frame.write_frame(encoder)

    @asyncio.coroutine
    def basic_qos_ok(self, frame):
        """"""
        pass

    @asyncio.coroutine
    def basic_cancel(self, consumer_tag, no_wait=False):
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CANCEL)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_shortstr(consumer_tag)
        encoder.write_bits(no_wait)
        frame.write_frame(encoder)

    @asyncio.coroutine
    def basic_cancel_ok(self, frame):
        pass

    @asyncio.coroutine
    def basic_get(self, queue_name='', no_ack=False):
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_GET)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_short(0)
        encoder.write_shortstr(queue_name)
        encoder.write_bits(no_ack)
        frame.write_frame(encoder)

    @asyncio.coroutine
    def basic_get_ok(self, frame):
        pass

    @asyncio.coroutine
    def basic_get_empty(self, frame):
        pass

    @asyncio.coroutine
    def basic_client_ack(self, delivery_tag, multiple=False):
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_ACK)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_long_long(delivery_tag)
        encoder.write_bits(multiple)
        frame.write_frame(encoder)

    @asyncio.coroutine
    def basic_server_ack(self, frame):
        pass

    @asyncio.coroutine
    def basic_reject(self, delivery_tag, requeue):
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_REJECT)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_long_long(delivery_tag)
        encoder.write_bits(requeue)
        frame.write_frame(encoder)

    @asyncio.coroutine
    def basic_client_nack(self, *args, **kwargs):
        pass

    @asyncio.coroutine
    def basic_server_nack(self, frame):
        pass

    @asyncio.coroutine
    def basic_recover_async(self):
        pass

    @asyncio.coroutine
    def basic_recover(self, *args, **kwargs):
        pass

    @asyncio.coroutine
    def basic_recover_ok(self, frame):
        pass

    @asyncio.coroutine
    def basic_publish(self, message):
        """publish"""
        pass

    @asyncio.coroutine
    def basic_consume(self, queue_name='', consumer_tag='', no_local=False, no_ack=False, exclusive=False,
                      no_wait=False, callback=None, arguments=None, on_cancel=None):
        if not arguments:
            arguments = {}
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CONSUME)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_short(0)
        encoder.write_shortstr(queue_name)
        encoder.write_shortstr(consumer_tag)
        encoder.write_bits(no_local, no_ack, exclusive, no_wait)
        encoder.write_table(arguments)

        self.message_queue = asyncio.Queue()
        frame.write_frame(encoder)

    @asyncio.coroutine
    def basic_consume_ok(self, frame):
        frame.frame()
        pass

    @asyncio.coroutine
    def consume(self, queue_name=''):
        assert self.message_queue, "No message_queue defined"
        message = yield from self.message_queue.get()
        return message

    @asyncio.coroutine
    def basic_deliver(self, frame):
        response = amqp_frame.AmqpDecoder(frame.payload)
        consumer_tag = response.read_shortstr()
        deliver_tag = response.read_long_long()
        content_header_frame = yield from self.protocol.get_frame()
        content_header_frame.frame()
        content_body_frame = yield from self.protocol.get_frame()
        content_body_frame.frame()
        yield from self.message_queue.put((consumer_tag, deliver_tag, content_body_frame.payload))
