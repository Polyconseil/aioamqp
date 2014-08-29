import unittest

import asyncio

from . import testcase
from . import testing
from .. import exceptions


class QueueDeclareTestCase(testcase.AmqpTestCase, unittest.TestCase):

    @asyncio.coroutine
    def _test_queue_declare(self, queue_name, exclusive=False, durable=False, auto_delete=False):
        # declare queue
        frame = yield from self.queue_declare(
            queue_name, no_wait=False, exclusive=exclusive, durable=durable,
            auto_delete=auto_delete)

        # assert returned frame has the good arguments
        # in test the channel declared queues with prefixed names, to get the full name of the
        # declared queue we have to use self.full_name function
        self.assertEqual(self.full_name(queue_name), frame.arguments['queue'])

        # retrieve queue info from rabbitmqctl
        queues = yield from self.list_queues()
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
        yield from self.create_channel()
        queue_name = "test_durable_and_auto_deleted"
        self.loop.run_until_complete(
            self._test_queue_declare(queue_name, exclusive=False, durable=True, auto_delete=True))

    def test_durable_and_not_auto_deleted(self):
        yield from self.create_channel()
        queue_name = "test_durable_and_not_auto_delete"
        self.loop.run_until_complete(
            self._test_queue_declare(queue_name, exclusive=False, durable=True, auto_delete=False))

    def test_not_durable_and_auto_deleted(self):
        yield from self.create_channel()
        queue_name = "test_not_durable_and_auto_delete"
        self.loop.run_until_complete(
            self._test_queue_declare(queue_name, exclusive=False, durable=False, auto_delete=True))

    def test_not_durable_and_not_auto_deleted(self):
        yield from self.create_channel()
        queue_name = "test_not_durable_and_not_auto_de"
        self.loop.run_until_complete(
            self._test_queue_declare(queue_name, exclusive=False, durable=False, auto_delete=False))

    @testing.coroutine
    def test_exclusive(self):
        yield from self.create_channel()
        queue_name = "test_exclusive"
        # create an exclusive queue
        yield from self.queue_declare(queue_name, exclusive=True)
        # consume it
        yield from self.channel.basic_consume(queue_name, no_wait=False)
        # create an other amqp connection
        amqp2 = yield from self.connect()
        channel = yield from self.create_channel(amqp=amqp2)
        # assert that this connection cannot connect to the queue
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.basic_consume(queue_name, no_wait=False)
        # amqp and channels are auto deleted by test case

    @testing.coroutine
    def test_not_exclusive(self):
        yield from self.create_channel()
        queue_name = "test_not_exclusive"
        # create a non-exclusive queue
        yield from self.queue_declare(queue_name, exclusive=False)
        # consume it
        yield from self.channel.basic_consume(queue_name, no_wait=False)
        # create an other amqp connection
        amqp2 = yield from self.connect()
        channel = yield from self.create_channel(amqp=amqp2)
        # assert that this connection can connect to the queue
        yield from channel.basic_consume(queue_name, no_wait=False)

    @testing.coroutine
    def test_passive(self):
        # passive mode will test if the queue is created:
        # it raises an Exception if the quue does not exists.
        yield from self.create_channel()
        queue_name = "test_passive_queue"

        # ask for non-existing queue
        channel = yield from self.create_channel()
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.queue_declare(queue_name, passive=True)
        # create queue
        yield from self.queue_declare(queue_name)
        # get info
        channel = yield from self.create_channel()
        frame = yield from channel.queue_declare(queue_name, passive=True)
        # We have to use the fully qualified name of the queue here
        # queue_name is just the local name for this test, see ProxyChannel
        # class for more information

