"""
    Amqp channel specification
"""

import asyncio
import logging
import uuid
import io
from itertools import count

from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions
from .envelope import Envelope


logger = logging.getLogger(__name__)


class Channel:

    def __init__(self, protocol, channel_id, on_error=None):
        self.protocol = protocol
        self.channel_id = channel_id
        self.consumer_queues = {}
        self.consumer_callbacks = {}
        self._on_error_callback = on_error
        self.response_future = None
        self.close_event = asyncio.Event()
        self.cancelled_consumers = set()
        self.last_consumer_tag = None
        self.publisher_confirms = False
        self.delivery_tag_iter = None  # used for mapping delivered messages to publisher confirms

        self._futures = {}
        self._ctag_events = {}

    def _set_waiter(self, rpc_name):
        if rpc_name in self._futures:
            raise exceptions.SynchronizationError("Waiter already exists")

        fut = asyncio.Future()
        self._futures[rpc_name] = fut
        return fut

    def _get_waiter(self, rpc_name):
        fut = self._futures.pop(rpc_name, None)
        if not fut:
            raise exceptions.SynchronizationError("Call %s didn't set a waiter" % rpc_name)
        return fut

    @property
    def is_open(self):
        return not self.close_event.is_set()

    def _close_channel(self):
        self.protocol.release_channel_id(self.channel_id)
        self.close_event.set()

    def connection_closed(self, server_code=None, server_reason=None):
        for future in self._futures.values():
            kwargs = {}
            if server_code is not None:
                kwargs['code'] = server_code
            if server_reason is not None:
                kwargs['message'] = server_reason
            exc = exceptions.ChannelClosed(**kwargs)
            future.set_exception(exc)

        self._close_channel()

    @asyncio.coroutine
    def dispatch_frame(self, frame):
        methods = {
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_OPEN_OK): self.open_ok,
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_FLOW_OK): self.flow_ok,
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_CLOSE_OK): self.close_ok,
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_CLOSE): self.server_channel_close,

            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DECLARE_OK): self.exchange_declare_ok,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_BIND_OK): self.exchange_bind_ok,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_UNBIND_OK): self.exchange_unbind_ok,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DELETE_OK): self.exchange_delete_ok,

            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DECLARE_OK): self.queue_declare_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DELETE_OK): self.queue_delete_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_BIND_OK): self.queue_bind_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_UNBIND_OK): self.queue_unbind_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_PURGE_OK): self.queue_purge_ok,

            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_QOS_OK): self.basic_qos_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CONSUME_OK): self.basic_consume_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CANCEL_OK): self.basic_cancel_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_GET_OK): self.basic_get_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_GET_EMPTY): self.basic_get_empty,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_DELIVER): self.basic_deliver,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CANCEL): self.server_basic_cancel,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_ACK): self.basic_server_ack,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_NACK): self.basic_server_nack,

            (amqp_constants.CLASS_CONFIRM, amqp_constants.CONFIRM_SELECT_OK): self.confirm_select_ok,
        }

        if (frame.class_id, frame.method_id) not in methods:
            raise NotImplementedError("Frame (%s, %s) is not implemented" % (frame.class_id, frame.method_id))
        yield from methods[(frame.class_id, frame.method_id)](frame)

    @asyncio.coroutine
    def _write_frame(self, frame, request, no_wait, timeout=None, no_check_open=False):
        if not self.is_open and not no_check_open:
            raise exceptions.ChannelClosed()
        frame.write_frame(request)

#
## Channel class implementation
#

    @asyncio.coroutine
    def open(self, timeout=None):
        """Open the channel on the server."""
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_OPEN)
        request = amqp_frame.AmqpEncoder()
        request.write_shortstr('')
        fut = self._set_waiter('open')
        try:
            yield from self._write_frame(frame, request, no_wait=False, timeout=timeout, no_check_open=True)
        except Exception:
            self._get_waiter('open')
            fut.cancel()
            raise
        yield from fut
        return fut.result()

    @asyncio.coroutine
    def open_ok(self, frame):
        self.close_event.clear()
        fut = self._get_waiter('open')
        fut.set_result(True)
        logger.debug("Channel openned")

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
            future = self._set_waiter('close')
            return (yield from future)

    @asyncio.coroutine
    def close_ok(self, frame):
        self._get_waiter('close').set_result(True)
        logger.info("Channel closed")
        self._close_channel()

    @asyncio.coroutine
    def server_channel_close(self, frame):
        results = {
            'reply_code': frame.payload_decoder.read_short(),
            'reply_text': frame.payload_decoder.read_shortstr(),
            'class_id': frame.payload_decoder.read_short(),
            'method_id': frame.payload_decoder.read_short(),
        }
        self.connection_closed(results['reply_code'], results['reply_text'])

    @asyncio.coroutine
    def flow(self, active, timeout=None):
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_FLOW)
        request = amqp_frame.AmqpEncoder()
        request.write_bits(active)
        fut = self._set_waiter('flow')
        try:
            yield from self._write_frame(frame, request, no_wait=False, timeout=timeout, no_check_open=True)
        except Exception:
            self._get_waiter('flow')
            fut.cancel()
            raise
        yield from fut
        return fut.result()

    @asyncio.coroutine
    def flow_ok(self, frame):
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        active = bool(decoder.read_octet())
        self.close_event.clear()
        fut = self._get_waiter('flow')
        fut.set_result({'active': active})

        logger.debug("Flow ok")

#
## Exchange class implementation
#

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

        if not no_wait:
            future = self._set_waiter('exchange_declare')
            try:
                yield from self._write_frame(frame, request, no_wait, timeout=timeout)
            except Exception:
                self._get_waiter('exchange_declare')
                future.cancel()
                raise
            return (yield from future)

        yield from self._write_frame(frame, request, no_wait, timeout=timeout)

    @asyncio.coroutine
    def exchange_declare_ok(self, frame):
        future = self._get_waiter('exchange_declare')
        future.set_result(True)
        logger.debug("Exchange declared")
        return future

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

        if not no_wait:
            future = self._set_waiter('exchange_delete')
            try:
                yield from self._write_frame(frame, request, no_wait, timeout=timeout)
            except Exception:
                self._get_waiter('exchange_delete')
                future.cancel()
                raise
            return (yield from future)

        yield from self._write_frame(frame, request, no_wait, timeout=timeout)

    @asyncio.coroutine
    def exchange_delete_ok(self, frame):
        future = self._get_waiter('exchange_delete')
        future.set_result(True)
        logger.debug("Exchange deleted")

    @asyncio.coroutine
    def exchange_bind(self, exchange_destination, exchange_source, routing_key,
                      no_wait=False, arguments=None, timeout=None):
        if arguments is None:
            arguments = {}
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_BIND)

        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(exchange_destination)
        request.write_shortstr(exchange_source)
        request.write_shortstr(routing_key)

        request.write_bits(no_wait)
        request.write_table(arguments)
        if not no_wait:
            future = self._set_waiter('exchange_bind')
            try:
                yield from self._write_frame(frame, request, no_wait, timeout=timeout)
            except Exception:
                self._get_waiter('exchange_bind')
                future.cancel()
                raise
            return (yield from future)

        yield from self._write_frame(frame, request, no_wait, timeout=timeout)

    @asyncio.coroutine
    def exchange_bind_ok(self, frame):
        future = self._get_waiter('exchange_bind')
        future.set_result(True)
        logger.debug("Exchange bound")

    @asyncio.coroutine
    def exchange_unbind(self, exchange_destination, exchange_source, routing_key, no_wait=False, arguments=None, timeout=None):
        if arguments is None:
            arguments = {}
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.EXCHANGE_UNBIND, amqp_constants.EXCHANGE_UNBIND)

        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(exchange_destination)
        request.write_shortstr(exchange_source)
        request.write_shortstr(routing_key)

        request.write_bits(no_wait)
        request.write_table(arguments)
        if not no_wait:
            future = self._set_waiter('exchange_unbind')
            try:
                yield from self._write_frame(frame, request, no_wait, timeout=timeout)
            except Exception:
                self._get_waiter('exchange_unbind')
                future.cancel()
                raise
            return (yield from future)

        yield from self._write_frame(frame, request, no_wait, timeout=timeout)

    @asyncio.coroutine
    def exchange_unbind_ok(self, frame):
        future = self._get_waiter('exchange_unbind')
        future.set_result(True)
        logger.debug("Exchange bound")

#
## Queue class implementation
#

    @asyncio.coroutine
    def queue_declare(self, queue_name, passive=False, durable=False,
                      exclusive=False, auto_delete=False, no_wait=False, arguments=None, timeout=None):
        if arguments is None:
            arguments = {}

        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DECLARE)
        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(queue_name)
        request.write_bits(passive, durable, exclusive, auto_delete, no_wait)
        request.write_table(arguments)
        if not no_wait:
            future = self._set_waiter('queue_declare')
            try:
                yield from self._write_frame(frame, request, no_wait, timeout=timeout)
            except Exception:
                self._get_waiter('queue_declare')
                future.cancel()
                raise
            return (yield from future)

        yield from self._write_frame(frame, request, no_wait, timeout=timeout)

    @asyncio.coroutine
    def queue_declare_ok(self, frame):
        results = {
            'queue': frame.payload_decoder.read_shortstr(),
            'message_count': frame.payload_decoder.read_long(),
            'consumer_count': frame.payload_decoder.read_long(),
        }
        future = self._get_waiter('queue_declare')
        future.set_result(results)
        logger.debug("Queue declared")


    @asyncio.coroutine
    def queue_delete(self, queue_name, if_unused=False, if_empty=False, no_wait=False, timeout=None):
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DELETE)

        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(queue_name)
        request.write_bits(if_unused, if_empty, no_wait)
        if not no_wait:
            future = self._set_waiter('queue_delete')
            try:
                yield from self._write_frame(frame, request, no_wait, timeout=timeout)
            except Exception:
                self._get_waiter('queue_delete')
                future.cancel()
                raise
            return (yield from future)

        yield from self._write_frame(frame, request, no_wait, timeout=timeout)

    @asyncio.coroutine
    def queue_delete_ok(self, frame):
        future = self._get_waiter('queue_delete')
        future.set_result(True)
        logger.debug("Queue deleted")

    @asyncio.coroutine
    def queue_bind(self, queue_name, exchange_name, routing_key, no_wait=False, arguments=None, timeout=None):
        """Bind a queue and a channel."""
        if arguments is None:
            arguments = {}
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_BIND)

        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(queue_name)
        request.write_shortstr(exchange_name)
        request.write_shortstr(routing_key)
        request.write_octet(int(no_wait))
        request.write_table(arguments)
        if not no_wait:
            future = self._set_waiter('queue_bind')
            try:
                yield from self._write_frame(frame, request, no_wait, timeout=timeout)
            except Exception:
                self._get_waiter('queue_bind')
                future.cancel()
                raise
            return (yield from future)

        yield from self._write_frame(frame, request, no_wait, timeout=timeout)

    @asyncio.coroutine
    def queue_bind_ok(self, frame):
        future = self._get_waiter('queue_bind')
        future.set_result(True)
        logger.debug("Queue bound")

    @asyncio.coroutine
    def queue_unbind(self, queue_name, exchange_name, routing_key, arguments=None, timeout=None):
        if arguments is None:
            arguments = {}
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_UNBIND)

        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(queue_name)
        request.write_shortstr(exchange_name)
        request.write_shortstr(routing_key)
        request.write_table(arguments)
        future = self._set_waiter('queue_unbind')
        try:
            yield from self._write_frame(frame, request, no_wait=False, timeout=timeout)
        except Exception:
            self._get_waiter('queue_unbind')
            future.cancel()
            raise
        return (yield from future)

    @asyncio.coroutine
    def queue_unbind_ok(self, frame):
        future = self._get_waiter('queue_unbind')
        future.set_result(True)
        logger.debug("Queue unbound")

    @asyncio.coroutine
    def queue_purge(self, queue_name, no_wait=False, timeout=None):
        frame = amqp_frame.AmqpRequest(self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_PURGE)

        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(queue_name)
        request.write_octet(int(no_wait))
        if not no_wait:
            future = self._set_waiter('queue_purge')
            yield from self._write_frame(frame, request, no_wait, timeout=timeout)
            return (yield from future)

        yield from self._write_frame(frame, request, no_wait, timeout=timeout)

    @asyncio.coroutine
    def queue_purge_ok(self, frame):
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        message_count = decoder.read_long()
        future = self._get_waiter('queue_purge')
        future.set_result({'message_count': message_count})

#
## Basic class implementation
#

    @asyncio.coroutine
    def basic_publish(self, payload, exchange_name, routing_key, properties=None, mandatory=False, immediate=False):
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

        # split the payload

        frame_max = self.protocol.server_frame_max
        for chunk in (payload[0+i:frame_max+i] for i in range(0, len(payload), frame_max)):

            content_frame = amqp_frame.AmqpRequest(
                self.protocol.writer, amqp_constants.TYPE_BODY, self.channel_id)
            content_frame.declare_class(amqp_constants.CLASS_BASIC)
            encoder = amqp_frame.AmqpEncoder()
            if isinstance(chunk, str):
                encoder.payload.write(chunk.encode())
            else:
                encoder.payload.write(chunk)
            content_frame.write_frame(encoder)

        yield from self.protocol.writer.drain()

    @asyncio.coroutine
    def basic_qos(self, prefetch_size, prefetch_count, connection_global, timeout=None):
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_QOS)
        request = amqp_frame.AmqpEncoder()
        request.write_long(prefetch_size)
        request.write_short(prefetch_count)
        request.write_bits(connection_global)

        future = self._set_waiter('basic_qos')
        yield from self._write_frame(frame, request, no_wait=False, timeout=timeout)
        return (yield from future)

    @asyncio.coroutine
    def basic_qos_ok(self, frame):
        future = self._get_waiter('basic_qos')
        future.set_result(True)
        logger.debug("Qos ok")


    @asyncio.coroutine
    def basic_client_nack(self, *args, **kwargs):
        pass

    @asyncio.coroutine
    def basic_server_nack(self, frame, delivery_tag=None):
        if delivery_tag is None:
            decoder = amqp_frame.AmqpDecoder(frame.payload)
            delivery_tag = decoder.read_long_long()
        fut = self._get_waiter('basic_server_ack_{}'.format(delivery_tag))
        logger.debug('Received nack for delivery tag {!r}'.format(delivery_tag))
        fut.set_exception(exceptions.PublishFailed(delivery_tag))

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
    def basic_consume(self, queue_name='', consumer_tag='', no_local=False, no_ack=False, exclusive=False,
                      no_wait=False, callback=None, arguments=None, on_cancel=None, timeout=None):
        # If a consumer tag was not passed, create one
        consumer_tag = consumer_tag or 'ctag%i.%s' % (self.channel_id, uuid.uuid4().hex)

        if arguments is None:
            arguments = {}

        if callback is None or not asyncio.iscoroutinefunction(callback):
            raise exceptions.ConfigurationError("basic_consume requires a coroutine callback")

        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CONSUME)
        request = amqp_frame.AmqpEncoder()
        request.write_short(0)
        request.write_shortstr(queue_name)
        request.write_shortstr(consumer_tag)
        request.write_bits(no_local, no_ack, exclusive, no_wait)
        request.write_table(arguments)

        self.consumer_callbacks[consumer_tag] = callback
        self.last_consumer_tag = consumer_tag

        if not no_wait:
            future = self._set_waiter('basic_consume')
            try:
                yield from self._write_frame(frame, request, no_wait=no_wait, timeout=timeout)
            except Exception:
                self._get_waiter('basic_consume')
                future.cancel()
                raise
            yield from future
            self._ctag_events[consumer_tag].set()
            return future.result()

        yield from self._write_frame(frame, request, no_wait=no_wait, timeout=timeout)
        return {'consumer_tag': consumer_tag}

    @asyncio.coroutine
    def basic_consume_ok(self, frame):
        ctag = frame.payload_decoder.read_shortstr()
        results = {
            'consumer_tag': ctag,
        }
        future = self._get_waiter('basic_consume')
        future.set_result(results)
        self._ctag_events[ctag] = asyncio.Event()

    @asyncio.coroutine
    def basic_deliver(self, frame):
        response = amqp_frame.AmqpDecoder(frame.payload)
        consumer_tag = response.read_shortstr()
        delivery_tag = response.read_long_long()
        is_redeliver = response.read_bit()
        exchange_name = response.read_shortstr()
        routing_key = response.read_shortstr()
        content_header_frame = yield from self.protocol.get_frame()

        buffer = io.BytesIO()
        while(buffer.tell() < content_header_frame.body_size):
            content_body_frame = yield from self.protocol.get_frame()
            buffer.write(content_body_frame.payload)

        body = buffer.getvalue()
        envelope = Envelope(consumer_tag, delivery_tag, exchange_name, routing_key, is_redeliver)
        properties = content_header_frame.properties

        callback = self.consumer_callbacks[consumer_tag]

        event = self._ctag_events.get(consumer_tag)
        if event:
            yield from event.wait()
            del self._ctag_events[consumer_tag]
        yield from callback(body, envelope, properties)

    @asyncio.coroutine
    def server_basic_cancel(self, frame):
        """From the server, means the server won't send anymore messages to this consumer."""
        consumer_tag = frame.arguments['consumer_tag']
        self.cancelled_consumers.add(consumer_tag)
        logger.info("consume cancelled received")

    @asyncio.coroutine
    def basic_cancel(self, consumer_tag, no_wait=False, timeout=None):
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CANCEL)
        request = amqp_frame.AmqpEncoder()
        request.write_shortstr(consumer_tag)
        request.write_bits(no_wait)
        if not no_wait:
            future = self._set_waiter('basic_cancel')
            try:
                yield from self._write_frame(frame, request, no_wait=no_wait, timeout=timeout)
            except Exception:
                self._get_waiter('basic_cancel')
                future.cancel()
                raise
            return (yield from future)

        yield from self._write_frame(frame, request, no_wait=no_wait, timeout=timeout)

    @asyncio.coroutine
    def basic_cancel_ok(self, frame):
        results = {
            'consumer_tag': frame.payload_decoder.read_shortstr(),
        }
        future = self._get_waiter('basic_cancel')
        future.set_result(results)
        logger.debug("Cancel ok")

    @asyncio.coroutine
    def basic_get(self, queue_name='', no_ack=False, timeout=None):
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_GET)
        request = amqp_frame.AmqpEncoder()
        request.write_short(0)
        request.write_shortstr(queue_name)
        request.write_bits(no_ack)
        future = self._set_waiter('basic_get')
        try:
            yield from self._write_frame(frame, request, no_wait=False, timeout=timeout)
        except Exception:
            self._get_waiter('basic_get')
            future.cancel()
            raise
        return (yield from future)

    @asyncio.coroutine
    def basic_get_ok(self, frame):
        data = {}
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        data['delivery_tag'] = decoder.read_long_long()
        data['redelivered'] = bool(decoder.read_octet())
        data['exchange_name'] = decoder.read_shortstr()
        data['routing_key'] = decoder.read_shortstr()
        data['message_count'] = decoder.read_long()
        content_header_frame = yield from self.protocol.get_frame()

        buffer = io.BytesIO()
        while(buffer.tell() < content_header_frame.body_size):
            content_body_frame = yield from self.protocol.get_frame()
            buffer.write(content_body_frame.payload)

        data['message'] = buffer.getvalue()
        future = self._get_waiter('basic_get')
        future.set_result(data)

    @asyncio.coroutine
    def basic_get_empty(self, frame):
        future = self._get_waiter('basic_get')
        future.set_exception(exceptions.EmptyQueue)

    @asyncio.coroutine
    def basic_client_ack(self, delivery_tag, multiple=False, timeout=None):
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_ACK)
        request = amqp_frame.AmqpEncoder()
        request.write_long_long(delivery_tag)
        request.write_bits(multiple)
        yield from self._write_frame(frame, request, no_wait=False, timeout=timeout)

    @asyncio.coroutine
    def basic_server_ack(self, frame):
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        delivery_tag = decoder.read_long_long()
        fut = self._get_waiter('basic_server_ack_{}'.format(delivery_tag))
        logger.debug('Received ack for delivery tag {!r}'.format(delivery_tag))
        fut.set_result(True)

    @asyncio.coroutine
    def basic_reject(self, delivery_tag, requeue=False, timeout=None):
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(
            amqp_constants.CLASS_BASIC, amqp_constants.BASIC_REJECT)
        request = amqp_frame.AmqpEncoder()
        request.write_long_long(delivery_tag)
        request.write_bits(requeue)
        yield from self._write_frame(frame, request, no_wait=False, timeout=timeout)

#
## convenient aliases
#
    queue = queue_declare
    exchange = exchange_declare

    @asyncio.coroutine
    def publish(self, payload, exchange_name, routing_key, properties=None, mandatory=False, immediate=False):
        assert len(payload) != 0, "Payload cannot be empty"
        if not self.is_open:
            raise exceptions.ChannelClosed()

        if self.publisher_confirms:
            delivery_tag = next(self.delivery_tag_iter)
            fut = self._set_waiter('basic_server_ack_{}'.format(delivery_tag))

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

        # split the payload

        frame_max = self.protocol.server_frame_max
        for chunk in (payload[0+i:frame_max+i] for i in range(0, len(payload), frame_max)):

            content_frame = amqp_frame.AmqpRequest(
                self.protocol.writer, amqp_constants.TYPE_BODY, self.channel_id)
            content_frame.declare_class(amqp_constants.CLASS_BASIC)
            encoder = amqp_frame.AmqpEncoder()
            if isinstance(chunk, str):
                encoder.payload.write(chunk.encode())
            else:
                encoder.payload.write(chunk)
            content_frame.write_frame(encoder)

        yield from self.protocol.writer.drain()

        if self.publisher_confirms:
            yield from fut

    @asyncio.coroutine
    def confirm_select(self, *, no_wait=False, timeout=None):
        if self.publisher_confirms:
            raise ValueError('publisher confirms already enabled')
        frame = amqp_frame.AmqpRequest(
            self.protocol.writer, amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_CONFIRM, amqp_constants.CONFIRM_SELECT)
        request = amqp_frame.AmqpEncoder()
        request.write_shortstr('')

        if not no_wait:
            future = self._set_waiter('confirm_select')
            try:
                yield from self._write_frame(frame, request, no_wait=no_wait, timeout=timeout)
            except Exception:
                self._get_waiter('confirm_select')
                future.cancel()
                raise
            return (yield from future)

        yield from self._write_frame(frame, request, no_wait=no_wait, timeout=timeout)

    @asyncio.coroutine
    def confirm_select_ok(self, frame):
        self.publisher_confirms = True
        self.delivery_tag_iter = count(1)
        fut = self._get_waiter('confirm_select')
        fut.set_result(True)
        logger.debug("Confirm selected")
