"""
    Amqp basic class tests
"""

import asyncio
import struct
import unittest

from . import testcase
from . import testing
from .. import exceptions
from .. import properties


class QosTestCase(testcase.RabbitTestCase, unittest.TestCase):

    @testing.coroutine
    def test_basic_qos_default_args(self):
        result = yield from self.channel.basic_qos()
        self.assertTrue(result)

    @testing.coroutine
    def test_basic_qos(self):
        result = yield from self.channel.basic_qos(
            prefetch_size=0,
            prefetch_count=100,
            connection_global=False)

        self.assertTrue(result)

    @testing.coroutine
    def test_basic_qos_prefetch_size(self):
        with self.assertRaises(exceptions.ChannelClosed) as cm:
            yield from self.channel.basic_qos(
                prefetch_size=10,
                prefetch_count=100,
                connection_global=False)

        self.assertEqual(cm.exception.code, 540)

    @testing.coroutine
    def test_basic_qos_wrong_values(self):
        with self.assertRaises(struct.error):
            yield from self.channel.basic_qos(
                prefetch_size=100000,
                prefetch_count=1000000000,
                connection_global=False)


class BasicCancelTestCase(testcase.RabbitTestCase, unittest.TestCase):

    @testing.coroutine
    def test_basic_cancel(self):

        @asyncio.coroutine
        def callback(channel, body, envelope, _properties):
            pass

        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        yield from self.channel.queue_declare(queue_name)
        yield from self.channel.exchange_declare(exchange_name, type_name='direct')
        yield from self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        result = yield from self.channel.basic_consume(callback, queue_name=queue_name)
        result = yield from self.channel.basic_cancel(result['consumer_tag'])

        result = yield from self.channel.publish("payload", exchange_name, routing_key='')

        yield from asyncio.sleep(5, loop=self.loop)

        result = yield from self.channel.queue_declare(queue_name, passive=True)
        self.assertEqual(result['message_count'], 1)
        self.assertEqual(result['consumer_count'], 0)


    @testing.coroutine
    def test_basic_cancel_unknown_ctag(self):
        result = yield from self.channel.basic_cancel("unknown_ctag")
        self.assertTrue(result)


class BasicGetTestCase(testcase.RabbitTestCase, unittest.TestCase):


    @testing.coroutine
    def test_basic_get(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        yield from self.channel.queue_declare(queue_name)
        yield from self.channel.exchange_declare(exchange_name, type_name='direct')
        yield from self.channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)

        yield from self.channel.publish("payload", exchange_name, routing_key=routing_key)

        result = yield from self.channel.basic_get(queue_name)
        self.assertEqual(result['routing_key'], routing_key)
        self.assertFalse(result['redelivered'])
        self.assertIn('delivery_tag', result)
        self.assertEqual(result['exchange_name'].split('.')[-1], exchange_name)
        self.assertEqual(result['message'], b'payload')
        self.assertIsInstance(result['properties'], properties.Properties)

    @testing.coroutine
    def test_basic_get_empty(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''
        yield from self.channel.queue_declare(queue_name)
        yield from self.channel.exchange_declare(exchange_name, type_name='direct')
        yield from self.channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)

        with self.assertRaises(exceptions.EmptyQueue):
            yield from self.channel.basic_get(queue_name)


class BasicDeliveryTestCase(testcase.RabbitTestCase, unittest.TestCase):


    @asyncio.coroutine
    def publish(self, queue_name, exchange_name, routing_key, payload):
        yield from self.channel.queue_declare(queue_name, exclusive=False, no_wait=False)
        yield from self.channel.exchange_declare(exchange_name, type_name='fanout')
        yield from self.channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)
        yield from self.channel.publish(payload, exchange_name, queue_name)



    @testing.coroutine
    def test_ack_message(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        yield from self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def qcallback(channel, body, envelope, _properties):
            qfuture.set_result(envelope)

        yield from self.channel.basic_consume(qcallback, queue_name=queue_name)
        envelope = yield from qfuture

        yield from qfuture
        yield from self.channel.basic_client_ack(envelope.delivery_tag)

    @testing.coroutine
    def test_basic_nack(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        yield from self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def qcallback(channel, body, envelope, _properties):
            yield from self.channel.basic_client_nack(
                envelope.delivery_tag, multiple=True, requeue=False
            )
            qfuture.set_result(True)

        yield from self.channel.basic_consume(qcallback, queue_name=queue_name)
        yield from qfuture

    @testing.coroutine
    def test_basic_nack_norequeue(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        yield from self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def qcallback(channel, body, envelope, _properties):
            yield from self.channel.basic_client_nack(envelope.delivery_tag, requeue=False)
            qfuture.set_result(True)

        yield from self.channel.basic_consume(qcallback, queue_name=queue_name)
        yield from qfuture

    @testing.coroutine
    def test_basic_nack_requeue(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        yield from self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = asyncio.Future(loop=self.loop)
        called = False

        @asyncio.coroutine
        def qcallback(channel, body, envelope, _properties):
            nonlocal called
            if not called:
                called = True
                yield from self.channel.basic_client_nack(envelope.delivery_tag, requeue=True)
            else:
                yield from self.channel.basic_client_ack(envelope.delivery_tag)
                qfuture.set_result(True)

        yield from self.channel.basic_consume(qcallback, queue_name=queue_name)
        yield from qfuture


    @testing.coroutine
    def test_basic_reject(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''
        yield from self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def qcallback(channel, body, envelope, _properties):
            qfuture.set_result(envelope)

        yield from self.channel.basic_consume(qcallback, queue_name=queue_name)
        envelope = yield from qfuture

        yield from self.channel.basic_reject(envelope.delivery_tag)
