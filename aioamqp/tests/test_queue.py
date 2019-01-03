"""
    Amqp queue class tests
"""

import asyncio
import unittest

from . import testcase
from . import testing
from .. import exceptions


class QueueDeclareTestCase(testcase.RabbitTestCase, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.consume_future = asyncio.Future(loop=self.loop)

    async def callback(self, body, envelope, properties):
        self.consume_future.set_result((body, envelope, properties))

    async def get_callback_result(self):
        await self.consume_future
        result = self.consume_future.result()
        self.consume_future = asyncio.Future(loop=self.loop)
        return result

    async def test_queue_declare_no_name(self):
        result = await self.channel.queue_declare()
        self.assertIsNotNone(result['queue'])

    async def test_queue_declare(self):
        queue_name = 'queue_name'
        result = await self.channel.queue_declare('queue_name')
        self.assertEqual(result['message_count'], 0)
        self.assertEqual(result['consumer_count'], 0)
        self.assertEqual(result['queue'].split('.')[-1], queue_name)
        self.assertTrue(result)

    async def test_queue_declare_passive(self):
        queue_name = 'queue_name'
        await self.channel.queue_declare('queue_name')
        result = await self.channel.queue_declare(queue_name, passive=True)
        self.assertEqual(result['message_count'], 0)
        self.assertEqual(result['consumer_count'], 0)
        self.assertEqual(result['queue'].split('.')[-1], queue_name)

    async def test_queue_declare_custom_x_message_ttl_32_bits(self):
        queue_name = 'queue_name'
        # 2147483648 == 10000000000000000000000000000000
        # in binary, meaning it is 32 bit long
        x_message_ttl = 2147483648
        result = await self.channel.queue_declare('queue_name', arguments={
            'x-message-ttl': x_message_ttl
        })
        self.assertEqual(result['message_count'], 0)
        self.assertEqual(result['consumer_count'], 0)
        self.assertEqual(result['queue'].split('.')[-1], queue_name)
        self.assertTrue(result)

    async def test_queue_declare_passive_nonexistant_queue(self):
        queue_name = 'queue_name'
        with self.assertRaises(exceptions.ChannelClosed) as cm:
            await self.channel.queue_declare(queue_name, passive=True)

        self.assertEqual(cm.exception.code, 404)

    async def test_wrong_parameter_queue(self):
        queue_name = 'queue_name'
        await self.channel.queue_declare(queue_name, exclusive=False, auto_delete=False)

        with self.assertRaises(exceptions.ChannelClosed) as cm:
            await self.channel.queue_declare(queue_name,
                passive=False, exclusive=True, auto_delete=True)

        self.assertEqual(cm.exception.code, 406)

    async def test_multiple_channel_same_queue(self):
        queue_name = 'queue_name'

        channel1 = await self.amqp.channel()
        channel2 = await self.amqp.channel()

        result = await channel1.queue_declare(queue_name, passive=False)
        self.assertEqual(result['message_count'], 0)
        self.assertEqual(result['consumer_count'], 0)
        self.assertEqual(result['queue'].split('.')[-1], queue_name)

        result = await channel2.queue_declare(queue_name, passive=False)
        self.assertEqual(result['message_count'], 0)
        self.assertEqual(result['consumer_count'], 0)
        self.assertEqual(result['queue'].split('.')[-1], queue_name)

    async def _test_queue_declare(self, queue_name, exclusive=False, durable=False, auto_delete=False):
        # declare queue
        result = await self.channel.queue_declare(
            queue_name, no_wait=False, exclusive=exclusive, durable=durable,
            auto_delete=auto_delete)

        # assert returned results has the good arguments
        # in test the channel declared queues with prefixed names, to get the full name of the
        # declared queue we have to use self.full_name function
        self.assertEqual(self.full_name(queue_name), result['queue'])

        queues = self.list_queues()
        queue = queues[queue_name]

        # assert queue has been declared witht the good arguments
        self.assertEqual(queue_name, queue['name'])
        self.assertEqual(0, queue['consumers'])
        self.assertEqual(0, queue['messages_ready'])
        self.assertEqual(auto_delete, queue['auto_delete'])
        self.assertEqual(durable, queue['durable'])

        # delete queue
        await self.safe_queue_delete(queue_name)

    def test_durable_and_auto_deleted(self):
        self.loop.run_until_complete(
            self._test_queue_declare('q', exclusive=False, durable=True, auto_delete=True))

    def test_durable_and_not_auto_deleted(self):
        self.loop.run_until_complete(
            self._test_queue_declare('q', exclusive=False, durable=True, auto_delete=False))

    def test_not_durable_and_auto_deleted(self):
        self.loop.run_until_complete(
            self._test_queue_declare('q', exclusive=False, durable=False, auto_delete=True))

    def test_not_durable_and_not_auto_deleted(self):
        self.loop.run_until_complete(
            self._test_queue_declare('q', exclusive=False, durable=False, auto_delete=False))

    async def test_exclusive(self):
        # create an exclusive queue
        await self.channel.queue_declare("q", exclusive=True)
        # consume it
        await self.channel.basic_consume(self.callback, queue_name="q", no_wait=False)
        # create an other amqp connection

        _transport, protocol = await self.create_amqp()
        channel = await self.create_channel(amqp=protocol)
        # assert that this connection cannot connect to the queue
        with self.assertRaises(exceptions.ChannelClosed):
            await channel.basic_consume(self.callback, queue_name="q", no_wait=False)
        # amqp and channels are auto deleted by test case

    async def test_not_exclusive(self):
        # create a non-exclusive queue
        await self.channel.queue_declare('q', exclusive=False)
        # consume it
        await self.channel.basic_consume(self.callback, queue_name='q', no_wait=False)
        # create an other amqp connection
        _transport, protocol = await self.create_amqp()
        channel = await self.create_channel(amqp=protocol)
        # assert that this connection can connect to the queue
        await channel.basic_consume(self.callback, queue_name='q', no_wait=False)


class QueueDeleteTestCase(testcase.RabbitTestCase, unittest.TestCase):


    async def test_delete_queue(self):
        queue_name = 'queue_name'
        await self.channel.queue_declare(queue_name)
        result = await self.channel.queue_delete(queue_name)
        self.assertTrue(result)

    async def test_delete_inexistant_queue(self):
        queue_name = 'queue_name'
        if self.server_version() < (3, 3, 5):
            with self.assertRaises(exceptions.ChannelClosed) as cm:
                result = await self.channel.queue_delete(queue_name)

            self.assertEqual(cm.exception.code, 404)

        else:
            result = await self.channel.queue_delete(queue_name)
            self.assertTrue(result)

class QueueBindTestCase(testcase.RabbitTestCase, unittest.TestCase):


    async def test_bind_queue(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'

        await self.channel.queue_declare(queue_name)
        await self.channel.exchange_declare(exchange_name, type_name='direct')

        result = await self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        self.assertTrue(result)

    async def test_bind_unexistant_exchange(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'


        await self.channel.queue_declare(queue_name)
        with self.assertRaises(exceptions.ChannelClosed) as cm:
            await self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        self.assertEqual(cm.exception.code, 404)

    async def test_bind_unexistant_queue(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'


        await self.channel.exchange_declare(exchange_name, type_name='direct')

        with self.assertRaises(exceptions.ChannelClosed) as cm:
            await self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        self.assertEqual(cm.exception.code, 404)

    async def test_unbind_queue(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'

        await self.channel.queue_declare(queue_name)
        await self.channel.exchange_declare(exchange_name, type_name='direct')

        await self.channel.queue_bind(queue_name, exchange_name, routing_key='')

        result = await self.channel.queue_unbind(queue_name, exchange_name, routing_key='')
        self.assertTrue(result)


class QueuePurgeTestCase(testcase.RabbitTestCase, unittest.TestCase):


    async def test_purge_queue(self):
        queue_name = 'queue_name'

        await self.channel.queue_declare(queue_name)
        result = await self.channel.queue_purge(queue_name)
        self.assertEqual(result['message_count'], 0)

    async def test_purge_queue_inexistant_queue(self):
        queue_name = 'queue_name'

        with self.assertRaises(exceptions.ChannelClosed) as cm:
            await self.channel.queue_purge(queue_name)
        self.assertEqual(cm.exception.code, 404)
