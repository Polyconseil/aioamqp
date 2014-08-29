import unittest

from . import testcase
from . import testing
from .. import exceptions


class ConsumeTestCase(testcase.AmqpTestCase, unittest.TestCase):


    @testing.coroutine
    def test_consume(self):
        queue_name = "test_consume"
        exchange_name = "test_consume"
        # declare
        yield from self.create_channel()
        yield from self.queue_declare(queue_name, exclusive=True, no_wait=False)
        yield from self.exchange_declare(exchange_name, "fanout")
        yield from self.channel.queue_bind(queue_name, exchange_name, routing_key='')

        # get a different channel
        channel = yield from self.create_channel()

        # publish
        yield from channel.publish("coucou", exchange_name, routing_key='')

        # start consume
        yield from channel.basic_consume(queue_name)

        # get one
        consume_data = yield from channel.consume()
        consumer_tag, delivery_tag, message = consume_data
        self.assertIsNotNone(consumer_tag)
        self.assertIsNotNone(delivery_tag)
        self.assertEqual(b"coucou", message)

    @testing.coroutine
    def test_big_consume(self):
        queue_name = "test_big_consume"
        exchange_name = "test_big_consume"
        # declare
        yield from self.create_channel()
        yield from self.queue_declare(queue_name, exclusive=True, no_wait=False)
        yield from self.exchange_declare(exchange_name, "fanout")
        yield from self.channel.queue_bind(queue_name, exchange_name, routing_key='')

        # get a different channel
        channel = yield from self.create_channel()

        # publish
        yield from channel.publish("a"*1000000, exchange_name, routing_key='')

        # start consume
        yield from channel.basic_consume(queue_name)

        # get one
        consume_data = yield from channel.consume()
        consumer_tag, delivery_tag, message = consume_data
        self.assertIsNotNone(consumer_tag)
        self.assertIsNotNone(delivery_tag)
        self.assertEqual(b"a"*1000000, message)

    def test_stuck(self):
        queue_name = "test_stuck"
        yield from self.create_channel()

        # declare
        yield from self.queue_declare(queue_name, exclusive=True, no_wait=False)

        # get a different channel
        channel = yield from self.create_channel()

        # start consume
        yield from channel.basic_consume(queue_name)

        # close
        yield from channel.close()

        # get one
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.consume()

    def test_consume_multiple_queues(self):
        first_queue = 'first_queue'
        second_queue = 'second_queue'
        exchange_name = 'test_consume_multiple_queues'
        yield from self.create_channel()

        yield from self.queue_declare(first_queue, exclusive=True, no_wait=False)
        yield from self.queue_declare(second_queue, exclusive=True, no_wait=False)
        yield from self.exchange_declare(exchange_name, "direct")
        yield from self.channel.queue_bind(first_queue, exchange_name, routing_key=first_queue)
        yield from self.channel.queue_bind(second_queue, exchange_name, routing_key=second_queue)

        # get a different channel
        channel = yield from self.create_channel()

        # start consumers
        ctag_q1 = yield from channel.basic_consume(first_queue)
        ctag_q2 = yield from channel.basic_consume(second_queue)

        # put message in q1
        yield from channel.publish("test_message_1", exchange_name, first_queue)

        # get it
        data = (yield from channel.consume(ctag_q1))
        consumer_tag, delivery_tag, payload = data
        self.assertEqual(ctag_q1, consumer_tag)
        self.assertIsNotNone(delivery_tag)
        self.assertEqual(b"test_message_1", payload)

        # put message in q2
        yield from channel.publish("test_message_2", exchange_name, second_queue)

        # get it
        consumer_tag, delivery_tag, payload = yield from channel.consume(ctag_q2)
        self.assertEqual(ctag_q2, consumer_tag)
        self.assertEqual(b"test_message_2", payload)

    @testing.coroutine
    def test_duplicate_consumer_tag(self):
        first_queue = 'first_queue_duplicate_consumer_tag'
        second_queue = 'first_queue_duplicate_consumer_tag'
        yield from self.create_channel()

        yield from self.queue_declare(first_queue, exclusive=True, no_wait=False)
        yield from self.queue_declare(second_queue, exclusive=True, no_wait=False)
        yield from self.channel.basic_consume(first_queue, consumer_tag='tag')
        with self.assertRaises(exceptions.DuplicateConsumerTag):
            yield from self.channel.basic_consume(second_queue, consumer_tag='tag')
