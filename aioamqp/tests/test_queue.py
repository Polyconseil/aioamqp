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

    @asyncio.coroutine
    def callback(self, body, envelope, properties):
        self.consume_future.set_result((body, envelope, properties))

    @asyncio.coroutine
    def get_callback_result(self):
        yield from self.consume_future
        result = self.consume_future.result()
        self.consume_future = asyncio.Future(loop=self.loop)
        return result

    @testing.coroutine
    def test_queue_declare_no_name(self):
        result = yield from self.channel.queue_declare()
        self.assertIsNotNone(result['queue'])

    @testing.coroutine
    def test_queue_declare(self):
        queue_name = 'queue_name'
        result = yield from self.channel.queue_declare('queue_name')
        self.assertEqual(result['message_count'], 0)
        self.assertEqual(result['consumer_count'], 0)
        self.assertEqual(result['queue'].split('.')[-1], queue_name)
        self.assertTrue(result)

    @testing.coroutine
    def test_queue_declare_passive(self):
        queue_name = 'queue_name'
        yield from self.channel.queue_declare('queue_name')
        result = yield from self.channel.queue_declare(queue_name, passive=True)
        self.assertEqual(result['message_count'], 0)
        self.assertEqual(result['consumer_count'], 0)
        self.assertEqual(result['queue'].split('.')[-1], queue_name)

    @testing.coroutine
    def test_queue_declare_passive_nonexistant_queue(self):
        queue_name = 'queue_name'
        with self.assertRaises(exceptions.ChannelClosed) as cm:
            yield from self.channel.queue_declare(queue_name, passive=True)

        self.assertEqual(cm.exception.code, 404)

    @testing.coroutine
    def test_wrong_parameter_queue(self):
        queue_name = 'queue_name'
        yield from self.channel.queue_declare(queue_name, exclusive=False, auto_delete=False)

        with self.assertRaises(exceptions.ChannelClosed) as cm:
            yield from self.channel.queue_declare(queue_name,
                passive=False, exclusive=True, auto_delete=True)

        self.assertEqual(cm.exception.code, 406)

    @testing.coroutine
    def test_multiple_channel_same_queue(self):
        queue_name = 'queue_name'

        channel1 = yield from self.amqp.channel()
        channel2 = yield from self.amqp.channel()

        result = yield from channel1.queue_declare(queue_name, passive=False)
        self.assertEqual(result['message_count'], 0)
        self.assertEqual(result['consumer_count'], 0)
        self.assertEqual(result['queue'].split('.')[-1], queue_name)

        result = yield from channel2.queue_declare(queue_name, passive=False)
        self.assertEqual(result['message_count'], 0)
        self.assertEqual(result['consumer_count'], 0)
        self.assertEqual(result['queue'].split('.')[-1], queue_name)

    @asyncio.coroutine
    def _test_queue_declare(self, queue_name, exclusive=False, durable=False, auto_delete=False):
        # declare queue
        result = yield from self.channel.queue_declare(
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
        yield from self.safe_queue_delete(queue_name)

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

    @testing.coroutine
    def test_exclusive(self):
        # create an exclusive queue
        yield from self.channel.queue_declare("q", exclusive=True)
        # consume it
        yield from self.channel.basic_consume(self.callback, queue_name="q", no_wait=False)
        # create an other amqp connection

        _transport, protocol = yield from self.create_amqp()
        channel = yield from self.create_channel(amqp=protocol)
        # assert that this connection cannot connect to the queue
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.basic_consume(self.callback, queue_name="q", no_wait=False)
        # amqp and channels are auto deleted by test case

    @testing.coroutine
    def test_not_exclusive(self):
        # create a non-exclusive queue
        yield from self.channel.queue_declare('q', exclusive=False)
        # consume it
        yield from self.channel.basic_consume(self.callback, queue_name='q', no_wait=False)
        # create an other amqp connection
        _transport, protocol = yield from self.create_amqp()
        channel = yield from self.create_channel(amqp=protocol)
        # assert that this connection can connect to the queue
        yield from channel.basic_consume(self.callback, queue_name='q', no_wait=False)


class QueueDeleteTestCase(testcase.RabbitTestCase, unittest.TestCase):


    @testing.coroutine
    def test_delete_queue(self):
        queue_name = 'queue_name'
        yield from self.channel.queue_declare(queue_name)
        result = yield from self.channel.queue_delete(queue_name)
        self.assertTrue(result)

    @testing.coroutine
    def test_delete_inexistant_queue(self):
        queue_name = 'queue_name'
        if self.server_version() < (3, 3, 5):
            with self.assertRaises(exceptions.ChannelClosed) as cm:
                result = yield from self.channel.queue_delete(queue_name)

            self.assertEqual(cm.exception.code, 404)

        else:
            result = yield from self.channel.queue_delete(queue_name)
            self.assertTrue(result)

class QueueBindTestCase(testcase.RabbitTestCase, unittest.TestCase):


    @testing.coroutine
    def test_bind_queue(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'

        yield from self.channel.queue_declare(queue_name)
        yield from self.channel.exchange_declare(exchange_name, type_name='direct')

        result = yield from self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        self.assertTrue(result)

    @testing.coroutine
    def test_bind_unexistant_exchange(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'


        yield from self.channel.queue_declare(queue_name)
        with self.assertRaises(exceptions.ChannelClosed) as cm:
            yield from self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        self.assertEqual(cm.exception.code, 404)

    @testing.coroutine
    def test_bind_unexistant_queue(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'


        yield from self.channel.exchange_declare(exchange_name, type_name='direct')

        with self.assertRaises(exceptions.ChannelClosed) as cm:
            yield from self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        self.assertEqual(cm.exception.code, 404)

    @testing.coroutine
    def test_unbind_queue(self):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'

        yield from self.channel.queue_declare(queue_name)
        yield from self.channel.exchange_declare(exchange_name, type_name='direct')

        yield from self.channel.queue_bind(queue_name, exchange_name, routing_key='')

        result = yield from self.channel.queue_unbind(queue_name, exchange_name, routing_key='')
        self.assertTrue(result)


class QueuePurgeTestCase(testcase.RabbitTestCase, unittest.TestCase):


    @testing.coroutine
    def test_purge_queue(self):
        queue_name = 'queue_name'

        yield from self.channel.queue_declare(queue_name)
        result = yield from self.channel.queue_purge(queue_name)
        self.assertEqual(result['message_count'], 0)

    @testing.coroutine
    def test_purge_queue_inexistant_queue(self):
        queue_name = 'queue_name'

        with self.assertRaises(exceptions.ChannelClosed) as cm:
            yield from self.channel.queue_purge(queue_name)
        self.assertEqual(cm.exception.code, 404)
