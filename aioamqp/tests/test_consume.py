
import asyncio
import unittest

from . import testcase
from . import testing
from .. import exceptions
from ..properties import Properties


class ConsumeTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True


    def setUp(self):
        super().setUp()
        self.consume_future = asyncio.Future(loop=self.loop)

    @asyncio.coroutine
    def callback(self, channel, body, envelope, properties):
        self.consume_future.set_result((body, envelope, properties))

    @asyncio.coroutine
    def get_callback_result(self):
        yield from self.consume_future
        result = self.consume_future.result()
        self.consume_future = asyncio.Future(loop=self.loop)
        return result


    def test_wrong_callback_argument(self):

        def badcallback():
            pass

        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # get a different channel
        channel = yield from self.create_channel()

        # publish
        yield from channel.publish("coucou", "e", routing_key='',)

        # assert there is a message to consume
        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

        yield from asyncio.sleep(2, loop=self.loop)
        # start consume
        with self.assertRaises(exceptions.ConfigurationError):
            yield from channel.basic_consume(badcallback, queue_name="q")

    @testing.coroutine
    def test_consume(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # get a different channel
        channel = yield from self.create_channel()

        # publish
        yield from channel.publish("coucou", "e", routing_key='',)

        # start consume
        yield from channel.basic_consume(self.callback, queue_name="q")

        # get one
        body, envelope, properties = yield from self.get_callback_result()
        self.assertIsNotNone(envelope.consumer_tag)
        self.assertIsNotNone(envelope.delivery_tag)
        self.assertEqual(b"coucou", body)
        self.assertIsInstance(properties, Properties)

    @testing.coroutine
    def test_big_consume(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')


        # get a different channel
        channel = yield from self.create_channel()

        # publish
        yield from channel.publish("a"*1000000, "e", routing_key='',)

        # start consume
        yield from channel.basic_consume(self.callback, queue_name="q")

        # get one
        body, envelope, properties = yield from self.get_callback_result()
        self.assertIsNotNone(envelope.consumer_tag)
        self.assertIsNotNone(envelope.delivery_tag)
        self.assertEqual(b"a"*1000000, body)
        self.assertIsInstance(properties, Properties)

    @testing.coroutine
    def test_consume_multiple_queues(self):
        yield from self.channel.queue_declare("q1", exclusive=True, no_wait=False)
        yield from self.channel.queue_declare("q2", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "direct")
        yield from self.channel.queue_bind("q1", "e", routing_key="q1")
        yield from self.channel.queue_bind("q2", "e", routing_key="q2")

        # get a different channel
        channel = yield from self.create_channel()

        q1_future = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def q1_callback(channel, body, envelope, properties):
            q1_future.set_result((body, envelope, properties))

        q2_future = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def q2_callback(channel, body, envelope, properties):
            q2_future.set_result((body, envelope, properties))

        # start consumers
        result = yield from channel.basic_consume(q1_callback, queue_name="q1")
        ctag_q1 = result['consumer_tag']
        result = yield from channel.basic_consume(q2_callback, queue_name="q2")
        ctag_q2 = result['consumer_tag']

        # put message in q1
        yield from channel.publish("coucou1", "e", "q1")

        # get it
        body1, envelope1, properties1 = yield from q1_future
        self.assertEqual(ctag_q1, envelope1.consumer_tag)
        self.assertIsNotNone(envelope1.delivery_tag)
        self.assertEqual(b"coucou1", body1)
        self.assertIsInstance(properties1, Properties)

        # put message in q2
        yield from channel.publish("coucou2", "e", "q2")

        # get it
        body2, envelope2, properties2 = yield from q2_future
        self.assertEqual(ctag_q2, envelope2.consumer_tag)
        self.assertEqual(b"coucou2", body2)
        self.assertIsInstance(properties2, Properties)

    @testing.coroutine
    def test_duplicate_consumer_tag(self):
        yield from self.channel.queue_declare("q1", exclusive=True, no_wait=False)
        yield from self.channel.queue_declare("q2", exclusive=True, no_wait=False)
        yield from self.channel.basic_consume(self.callback, queue_name="q1", consumer_tag='tag')

        with self.assertRaises(exceptions.ChannelClosed) as cm:
            yield from self.channel.basic_consume(self.callback, queue_name="q2", consumer_tag='tag')

        self.assertEqual(cm.exception.code, 530)

    @testing.coroutine
    def test_consume_callaback_synced(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # get a different channel
        channel = yield from self.create_channel()

        # publish
        yield from channel.publish("coucou", "e", routing_key='',)

        sync_future = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def callback(channel, body, envelope, properties):
            self.assertTrue(sync_future.done())

        yield from channel.basic_consume(callback, queue_name="q")
        sync_future.set_result(True)
