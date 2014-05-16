import unittest

import asyncio

from . import testcase
from . import testing
from .. import exceptions


class QueueDeclareTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

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
        yield from self.queue_declare("q", exclusive=True)
        # consume it
        yield from self.channel.basic_consume("q", no_wait=False)
        # create an other amqp connection
        amqp2 = yield from self.create_amqp()
        channel = yield from self.create_channel(amqp=amqp2)
        # assert that this connection cannot connect to the queue
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.basic_consume("q", no_wait=False)
        # amqp and channels are auto deleted by test case

    @testing.coroutine
    def test_not_exclusive(self):
            # create a non-exclusive queue
            yield from self.queue_declare('q', exclusive=False)
            # consume it
            yield from self.channel.basic_consume('q', no_wait=False)
            # create an other amqp connection
            amqp2 = yield from self.create_amqp()
            channel = yield from self.create_channel(amqp=amqp2)
            # assert that this connection can connect to the queue
            yield from channel.basic_consume('q', no_wait=False)

    @testing.coroutine
    def test_passive(self):
        yield from self.safe_queue_delete('q')
        # ask for non-existing queue
        channel = yield from self.create_channel()
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.queue_declare('q', passive=True)
        # create queue
        yield from self.queue_declare('q')
        # get info
        channel = yield from self.create_channel()
        frame = yield from channel.queue_declare('q', passive=True)
        # We have to use the fully qualified name of the queue here
        # 'q' is just the local name for this test, see ProxyChannel
        # class for more information
        self.assertEqual(self.full_name('q'), frame.arguments['queue'])
