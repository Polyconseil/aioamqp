import unittest

from . import testcase
from . import testing
from .. import exceptions


class ConsumeTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_consume(self):
        # declare
        yield from self.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # get a different channel
        channel = yield from self.create_channel()

        # publish
        yield from channel.publish("coucou", "e", routing_key='')

        # assert there is a message to consume
        queues = yield from self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

        # start consume
        yield from channel.basic_consume("q")

        # get one
        consume_data = yield from channel.consume()
        consumer_tag, delivery_tag, message = consume_data
        self.assertIsNotNone(consumer_tag)
        self.assertIsNotNone(delivery_tag)
        self.assertEqual(b"coucou", message)

    @testing.coroutine
    def test_big_consume(self):
        # declare
        yield from self.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # get a different channel
        channel = yield from self.create_channel()

        # publish
        yield from channel.publish("a"*1000000, "e", routing_key='')

        # assert there is a message to consume
        queues = yield from self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

        # start consume
        yield from channel.basic_consume("q")

        # get one
        consume_data = yield from channel.consume()
        consumer_tag, delivery_tag, message = consume_data
        self.assertIsNotNone(consumer_tag)
        self.assertIsNotNone(delivery_tag)
        self.assertEqual(b"a"*1000000, message)

    @testing.coroutine
    def test_stuck(self):
        # declare
        yield from self.queue_declare("q", exclusive=True, no_wait=False)

        # get a different channel
        channel = yield from self.create_channel()

        # start consume
        yield from channel.basic_consume("q")

        # close
        yield from channel.close()

        # get one
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.consume()

    @testing.coroutine
    def test_consume_multiple_queues(self):
        yield from self.queue_declare("q1", exclusive=True, no_wait=False)
        yield from self.queue_declare("q2", exclusive=True, no_wait=False)
        yield from self.exchange_declare("e", "direct")
        yield from self.channel.queue_bind("q1", "e", routing_key="q1")
        yield from self.channel.queue_bind("q2", "e", routing_key="q2")

        # get a different channel
        channel = yield from self.create_channel()

        # start consumers
        frame = yield from channel.basic_consume("q1")
        ctag_q1 = frame.arguments['consumer_tag']
        frame = yield from channel.basic_consume("q2")
        ctag_q2 = frame.arguments['consumer_tag']

        # put message in q1
        yield from channel.publish("coucou1", "e", "q1")

        # get it
        data = (yield from channel.consume(ctag_q1))
        consumer_tag, delivery_tag, payload = data
        self.assertEqual(ctag_q1, consumer_tag)
        self.assertIsNotNone(delivery_tag)
        self.assertEqual(b"coucou1", payload)

        # put message in q2
        yield from channel.publish("coucou2", "e", "q2")

        # get it
        consumer_tag, delivery_tag, payload = yield from channel.consume(ctag_q2)
        self.assertEqual(ctag_q2, consumer_tag)
        self.assertEqual(b"coucou2", payload)

    @testing.coroutine
    def test_duplicate_consumer_tag(self):
        yield from self.queue_declare("q1", exclusive=True, no_wait=False)
        yield from self.queue_declare("q2", exclusive=True, no_wait=False)
        yield from self.channel.basic_consume("q1", consumer_tag='tag')
        with self.assertRaises(exceptions.DuplicateConsumerTag):
            yield from self.channel.basic_consume("q2", consumer_tag='tag')
