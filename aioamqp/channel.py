"""
    Amqp channel specification
"""

import asyncio
import logging
import uuid

from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions

logger = logging.getLogger(__name__)


class Channel:

    def __init__(self, protocol, channel_id):
        self.protocol = protocol
        self.channel_id = channel_id
        self.consumer_queues = {}
        self.response_future = None
        self.close_event = asyncio.Event()
        self.cancelled_consumers = set()
        self.last_consumer_tag = None

    @property
    def is_open(self):
        return not self.close_event.is_set()

    @asyncio.coroutine
    def open(self, timeout=None):
        """Open the channel on the server."""
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_OPEN)
        request = amqp_frame.AmqpEncoder()
        request.write_shortstr('')
        return (yield from self._write_frame(frame, request, no_wait=False, timeout=timeout, no_check_open=True))

    @asyncio.coroutine
    def open_ok(self, frame):
        self.close_event.clear()
        if self.response_future is not None:
            self.response_future.set_result(frame)
        frame.frame()
        logger.info("channel opened")

    @asyncio.coroutine
    def close(self, reply_code=0, reply_text="Normal Shutdown", no_wait=False, timeout=None):
        """Close the channel."""
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_CLOSE)
        request = amqp_frame.AmqpEncoder()
        request.write_short(reply_code)
        request.write_shortstr(reply_text)
        request.write_short(0)
        request.write_short(0)
        frame.write_frame(request)
        if not no_wait:
            yield from self.wait_closed(timeout=timeout)

    @asyncio.coroutine
    def wait_closed(self, timeout=None):
        yield from asyncio.wait_for(self.close_event.wait(), timeout=timeout)

    @asyncio.coroutine
    def close_ok(self, frame):
        self.close_event.set()
        self._close_consumers()
        frame.frame()
        logger.info("channel closed")

    @asyncio.coroutine
    def dispatch_frame(self, frame):
        methods = {
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_OPEN_OK): self.open_ok,
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_CLOSE_OK): self.close_ok,
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_CLOSE): self.server_channel_close,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DECLARE_OK): self.exchange_declare_ok,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DELETE_OK): self.exchange_delete_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DECLARE_OK): self.queue_declare_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DELETE_OK): self.queue_delete_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_BIND_OK): self.queue_bind_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CONSUME_OK): self.basic_consume_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_DELIVER): self.basic_deliver,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CANCEL): self.server_basic_cancel,
        }
        try:
            yield from methods[(frame.class_id, frame.method_id)](frame)
        except KeyError as ex:
            raise NotImplementedError("Frame (%s, %s) is not implemented" % (frame.class_id, frame.method_id)) from ex

    @asyncio.coroutine
    def _write_frame(self, frame, request, no_wait, timeout=None, no_check_open=False):
        if not self.is_open and not no_check_open:
            raise exceptions.ChannelClosed()
        if no_wait:
            frame.write_frame(request)
        else:
            assert self.response_future is None, "Already waiting for some event"
            self.response_future = asyncio.Future()
            frame.write_frame(request)
            try:
                response_frame = yield from asyncio.wait_for(self.response_future, timeout=timeout)
            finally:
                self.response_future = None
            return response_frame

    @asyncio.coroutine
    def exchange_declare(self, exchange_name, type_name, passive=False, durable=False,
                         auto_delete=False, no_wait=False, arguments=None, timeout=None):
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DECLARE)
        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(exchange_name)
        request.write_shortstr(type_name)

        internal = False  # internal: deprecated
        request.write_bits(passive, durable, auto_delete, internal, no_wait)
        request.write_table(arguments)

        return (yield from self._write_frame(frame, request, no_wait, timeout=timeout))

    @asyncio.coroutine
    def exchange_declare_ok(self, frame):
        if self.response_future is not None:
            self.response_future.set_result(frame)
        frame.frame()
        logger.debug("exchange declared")

    @asyncio.coroutine
    def exchange_delete(self, exchange_name, if_unused=False, no_wait=False, timeout=None):
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DELETE)
        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(exchange_name)
        request.write_bits(if_unused, no_wait)
        return (yield from self._write_frame(frame, request, no_wait, timeout=timeout))

    @asyncio.coroutine
    def exchange_delete_ok(self, frame):
        if self.response_future is not None:
            self.response_future.set_result(frame)
        frame.frame()
        logger.debug("exchange deleted")

    @asyncio.coroutine
    def queue_declare(self, queue_name, passive=False, durable=False,
                      exclusive=False, auto_delete=False, no_wait=False, arguments=None, timeout=None):
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DECLARE)

        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(queue_name)
        request.write_bits(passive, durable, exclusive, auto_delete, no_wait)
        request.write_table({})
        return (yield from self._write_frame(frame, request, no_wait, timeout=timeout))

    @asyncio.coroutine
    def queue_declare_ok(self, frame):
        if self.response_future is not None:
            self.response_future.set_result(frame)
        frame.frame()
        logger.debug("queue declared")

    @asyncio.coroutine
    def queue_delete(self, queue_name, if_unused=False, if_empty=False, no_wait=False, timeout=None):
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DELETE)

        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(queue_name)
        request.write_bits(if_unused, if_empty, no_wait)
        return (yield from self._write_frame(frame, request, no_wait, timeout=timeout))

    @asyncio.coroutine
    def queue_delete_ok(self, frame):
        if self.response_future is not None:
            self.response_future.set_result(frame)
        frame.frame()
        logger.debug("queue deleted")

    @asyncio.coroutine
    def server_channel_close(self, frame):
        if self.response_future is not None:
            exc_msg = "{} ({})".format(frame.arguments['reply_text'], frame.arguments['reply_code'])
            self.response_future.set_exception(exceptions.ChannelClosed(exc_msg, frame=frame))
        frame.frame()
        self.close_event.set()
        self.close_consumers()

    def _close_consumers(self, exc=None):
        exc = exc or exceptions.ChannelClosed()
        for queue in self.consumer_queues.values():
            queue.put_nowait(exc)

#
## Public api
#

    queue = queue_declare
    exchange = exchange_declare

    @asyncio.coroutine
    def queue_bind(self, queue_name, exchange_name, routing_key, no_wait=False, arguments=None, timeout=None):
        """Bind a queue and a channel."""
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
        return (yield from self._write_frame(frame, request, no_wait, timeout=timeout))

    @asyncio.coroutine
    def queue_bind_ok(self, frame):
        if self.response_future is not None:
            self.response_future.set_result(frame)
        frame.frame()
        logger.debug("queue bound")

    @asyncio.coroutine
    def exchange_bind(self, exchange_source, exchange_destination, routing_key, no_wait=False, arguments=None):
        """Bind two exhanges together."""
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
        assert len(payload) != 0, "Payload cannot be empty"
        if not self.is_open:
            raise exceptions.ChannelClosed()

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
        pass

    @asyncio.coroutine
    def basic_consume(self, queue_name='', consumer_tag='', no_local=False, no_ack=False, exclusive=False,
                      no_wait=False, callback=None, arguments=None, on_cancel=None, timeout=None):
        # If a consumer tag was not passed, create one
        consumer_tag = consumer_tag or 'ctag%i.%s' % (self.channel_id, uuid.uuid4().hex)

        if consumer_tag in self.consumer_queues or consumer_tag in self.cancelled_consumers:
            raise exceptions.DuplicateConsumerTag(consumer_tag)

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

        self.consumer_queues[consumer_tag] = asyncio.Queue()
        self.last_consumer_tag = consumer_tag

        if no_wait:
            return consumer_tag
        else:
            return (yield from self._write_frame(frame, encoder, no_wait, timeout=timeout))

    @asyncio.coroutine
    def basic_consume_ok(self, frame):
        if self.response_future is not None:
            self.response_future.set_result(frame)
        frame.frame()
        logger.debug("basic consume ok received")

    @asyncio.coroutine
    def consume(self, consumer_tag=None):
        """Wait for a message and returns it.

        If consumer_tag is None, then the last consumer_tag declared in basic_consume will be used.
        """
        consumer_tag = consumer_tag or self.last_consumer_tag
        if consumer_tag not in self.consumer_queues and consumer_tag in self.cancelled_consumers:
            raise exceptions.ConsumerCancelled(consumer_tag)
        if not self.is_open:
            raise exceptions.ChannelClosed()
        data = yield from self.consumer_queues[consumer_tag].get()
        if isinstance(data, exceptions.AioamqpException):
            if self.consumer_queues[consumer_tag].qsize() == 0:
                del self.consumer_queues[consumer_tag]
            raise data
        return data

    @asyncio.coroutine
    def basic_deliver(self, frame):
        response = amqp_frame.AmqpDecoder(frame.payload)
        consumer_tag = response.read_shortstr()
        deliver_tag = response.read_long_long()
        content_header_frame = yield from self.protocol.get_frame()
        content_header_frame.frame()
        content_body_frame = yield from self.protocol.get_frame()
        content_body_frame.frame()
        self.consumer_queues[consumer_tag].put_nowait((consumer_tag, deliver_tag, content_body_frame.payload))

    @asyncio.coroutine
    def server_basic_cancel(self, frame):
        """From the server, means the server won't send anymore messages to this consumer."""
        consumer_tag = frame.arguments['consumer_tag']
        self.cancelled_consumers.add(consumer_tag)
        if consumer_tag in self.consumer_queues:
            self.consumer_queues[consumer_tag].put_nowait(exceptions.ConsumerCancelled(consumer_tag))
        frame.frame()
        logger.info("consume cancelled received")
