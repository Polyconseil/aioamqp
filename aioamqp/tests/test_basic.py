"""
    Amqp basic class tests
"""

import asyncio
import asynctest

from . import testcase
from .. import exceptions
from .. import properties


class QosTestCase(testcase.RabbitTestCaseMixin, asynctest.TestCase):

    async def test_basic_qos_default_args(self):
        result = await self.channel.basic_qos()
        self.assertTrue(result)

    async def test_basic_qos(self):
        result = await self.channel.basic_qos(
            prefetch_size=0,
            prefetch_count=100,
            connection_global=False)

        self.assertTrue(result)

    async def test_basic_qos_prefetch_size(self):
        with self.assertRaises(exceptions.ChannelClosed) as cm:
            await self.channel.basic_qos(
                prefetch_size=10,
                prefetch_count=100,
                connection_global=False)

        self.assertEqual(cm.exception.code, 540)

    async def test_basic_qos_wrong_values(self):
        with self.assertRaises(TypeError):
            await self.channel.basic_qos(
                prefetch_size=100000,
                prefetch_count=1000000000,
                connection_global=False)


class BasicCancelTestCase(testcase.RabbitTestCaseMixin, asynctest.TestCase):

    async def test_basic_cancel(self):

        async def callback(channel, body, envelope, _properties):
            pass

        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        await self.channel.queue_declare(queue_name)
        await self.channel.exchange_declare(exchange_name, type_name='direct')
        await self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        result = await self.channel.basic_consume(callback, queue_name=queue_name)
        result = await self.channel.basic_cancel(result['consumer_tag'])

        result = await self.channel.publish("payload", exchange_name, routing_key='')

        await asyncio.sleep(5)

        result = await self.channel.queue_declare(queue_name, passive=True)
        self.assertEqual(result['message_count'], 1)
        self.assertEqual(result['consumer_count'], 0)


    async def test_basic_cancel_unknown_ctag(self):
        result = await self.channel.basic_cancel("unknown_ctag")
        self.assertTrue(result)


class BasicGetTestCase(testcase.RabbitTestCaseMixin, asynctest.TestCase):


    async def test_basic_get(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.channel.queue_declare(queue_name)
        await self.channel.exchange_declare(exchange_name, type_name='direct')
        await self.channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)

        await self.channel.publish("payload", exchange_name, routing_key=routing_key)

        result = await self.channel.basic_get(queue_name)
        self.assertEqual(result['routing_key'], routing_key)
        self.assertFalse(result['redelivered'])
        self.assertIn('delivery_tag', result)
        self.assertEqual(result['exchange_name'].split('.')[-1], exchange_name)
        self.assertEqual(result['message'], b'payload')
        self.assertIsInstance(result['properties'], properties.Properties)

    async def test_basic_get_empty(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''
        await self.channel.queue_declare(queue_name)
        await self.channel.exchange_declare(exchange_name, type_name='direct')
        await self.channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)

        with self.assertRaises(exceptions.EmptyQueue):
            await self.channel.basic_get(queue_name)


class BasicDeliveryTestCase(testcase.RabbitTestCaseMixin, asynctest.TestCase):


    async def publish(self, queue_name, exchange_name, routing_key, payload):
        await self.channel.queue_declare(queue_name, exclusive=False, no_wait=False)
        await self.channel.exchange_declare(exchange_name, type_name='fanout')
        await self.channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)
        await self.channel.publish(payload, exchange_name, queue_name)



    async def test_ack_message(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = asyncio.Future(loop=self.loop)

        async def qcallback(channel, body, envelope, _properties):
            qfuture.set_result(envelope)

        await self.channel.basic_consume(qcallback, queue_name=queue_name)
        envelope = await qfuture

        await qfuture
        await self.channel.basic_client_ack(envelope.delivery_tag)

    async def test_basic_nack(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = asyncio.Future(loop=self.loop)

        async def qcallback(channel, body, envelope, _properties):
            await self.channel.basic_client_nack(
                envelope.delivery_tag, multiple=True, requeue=False
            )
            qfuture.set_result(True)

        await self.channel.basic_consume(qcallback, queue_name=queue_name)
        await qfuture

    async def test_basic_nack_norequeue(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = asyncio.Future(loop=self.loop)

        async def qcallback(channel, body, envelope, _properties):
            await self.channel.basic_client_nack(envelope.delivery_tag, requeue=False)
            qfuture.set_result(True)

        await self.channel.basic_consume(qcallback, queue_name=queue_name)
        await qfuture

    async def test_basic_nack_requeue(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = asyncio.Future(loop=self.loop)
        called = False

        async def qcallback(channel, body, envelope, _properties):
            nonlocal called
            if not called:
                called = True
                await self.channel.basic_client_nack(envelope.delivery_tag, requeue=True)
            else:
                await self.channel.basic_client_ack(envelope.delivery_tag)
                qfuture.set_result(True)

        await self.channel.basic_consume(qcallback, queue_name=queue_name)
        await qfuture


    async def test_basic_reject(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''
        await self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = asyncio.Future(loop=self.loop)

        async def qcallback(channel, body, envelope, _properties):
            qfuture.set_result(envelope)

        await self.channel.basic_consume(qcallback, queue_name=queue_name)
        envelope = await qfuture

        await self.channel.basic_reject(envelope.delivery_tag)
