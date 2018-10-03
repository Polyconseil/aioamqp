"""
    Server received requests handling tests

"""

import asyncio
import unittest.mock
import uuid

from . import testcase
from . import testing


@asyncio.coroutine
def consumer(channel, body, envelope, properties):
    yield from channel.basic_client_ack(envelope.delivery_tag)


class ServerBasicCancelTestCase(testcase.RabbitTestCase, unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super().setUp()
        self.queue_name = str(uuid.uuid4())

    @testing.coroutine
    def test_cancel_whilst_consuming(self):
        yield from self.channel.queue_declare(self.queue_name)

        # None is non-callable.  We want to make sure the callback is
        # unregistered and never called.
        yield from self.channel.basic_consume(None)
        yield from self.channel.queue_delete(self.queue_name)

    @testing.coroutine
    def test_cancel_callbacks(self):
        callback_calls = []

        @asyncio.coroutine
        def coroutine_callback(*args, **kwargs):
            callback_calls.append((args, kwargs))

        def function_callback(*args, **kwargs):
            callback_calls.append((args, kwargs))

        self.channel.add_cancellation_callback(coroutine_callback)
        self.channel.add_cancellation_callback(function_callback)

        yield from self.channel.queue_declare(self.queue_name)
        rv = yield from self.channel.basic_consume(consumer)
        yield from self.channel.queue_delete(self.queue_name)

        self.assertEqual(2, len(callback_calls))
        for args, kwargs in callback_calls:
            self.assertIs(self.channel, args[0])
            self.assertEqual(rv['consumer_tag'], args[1])

    @testing.coroutine
    def test_cancel_callback_exceptions(self):
        callback_calls = []

        def function_callback(*args, **kwargs):
            callback_calls.append((args, kwargs))
            raise RuntimeError

        self.channel.add_cancellation_callback(function_callback)
        self.channel.add_cancellation_callback(function_callback)

        yield from self.channel.queue_declare(self.queue_name)
        yield from self.channel.basic_consume(consumer)
        yield from self.channel.queue_delete(self.queue_name)

        self.assertEqual(2, len(callback_calls))
        self.assertTrue(self.channel.is_open)
