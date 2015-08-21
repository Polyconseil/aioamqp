"""
    Amqp exchange class tests
"""

import asyncio
import unittest

from . import testcase
from . import testing
from .. import exceptions


class ExchangeDeclareTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_exchange_direct_declare(self):
        result = yield from self.channel.exchange_declare(
            'exchange_name', type_name='direct')
        self.assertTrue(result)

    @testing.coroutine
    def test_exchange_fanout_declare(self):
        result = yield from self.channel.exchange_declare(
            'exchange_name', type_name='fanout')
        self.assertTrue(result)

    @testing.coroutine
    def test_exchange_topic_declare(self):
        result = yield from self.channel.exchange_declare(
            'exchange_name', type_name='topic')
        self.assertTrue(result)

    @testing.coroutine
    def test_exchange_headers_declare(self):
        result = yield from self.channel.exchange_declare(
            'exchange_name', type_name='headers')
        self.assertTrue(result)

    @testing.coroutine
    def test_exchange_declare_wrong_types(self):
        result = yield from self.channel.exchange_declare(
            'exchange_name', type_name='headers',
            auto_delete=True, durable=True)
        self.assertTrue(result)

        with self.assertRaises(exceptions.ChannelClosed):
            result = yield from self.channel.exchange_declare(
                'exchange_name', type_name='fanout',
                auto_delete=False, durable=False)

    @testing.coroutine
    def test_exchange_declare_passive(self):
        result = yield from self.channel.exchange_declare(
            'exchange_name', type_name='headers',
            auto_delete=True, durable=True)
        self.assertTrue(result)
        result = yield from self.channel.exchange_declare(
            'exchange_name', type_name='headers',
            auto_delete=True, durable=True, passive=True)
        self.assertTrue(result)

        result = yield from self.channel.exchange_declare(
            'exchange_name', type_name='headers',
            auto_delete=False, durable=False, passive=True)
        self.assertTrue(result)


    @testing.coroutine
    def test_exchange_declare_passive_does_not_exists(self):
        with self.assertRaises(exceptions.ChannelClosed) as cm:
            result = yield from self.channel.exchange_declare(
                'non_existant_exchange',
                type_name='headers',
                auto_delete=False, durable=False, passive=True)
        self.assertEqual(cm.exception.code, 404)

    @asyncio.coroutine
    def test_exchange_declare_unknown_type(self):
        with self.assertRaises(exceptions.ChannelClosed):
            result = yield from self.channel.exchange_declare(
                'non_existant_exchange',
                type_name='unknown_type',
                auto_delete=False, durable=False, passive=True)


class ExchangeDelete(testcase.RabbitTestCase, unittest.TestCase):

    @testing.coroutine
    def test_delete(self):
        exchange_name = 'exchange_name'
        yield from self.channel.exchange_declare(exchange_name, type_name='direct')
        result = yield from self.channel.exchange_delete(exchange_name)
        self.assertTrue(result)
        with self.assertRaises(exceptions.ChannelClosed) as cm:
            yield from self.channel.exchange_declare(
                exchange_name, type_name='direct', passive=True
            )

        self.assertEqual(cm.exception.code, 404)


    @testing.coroutine
    def test_double_delete(self):
        exchange_name = 'exchange_name'
        yield from self.channel.exchange_declare(exchange_name, type_name='direct')
        result = yield from self.channel.exchange_delete(exchange_name)
        self.assertTrue(result)
        if self.server_version() < (3, 3, 5):
            with self.assertRaises(exceptions.ChannelClosed) as cm:
                yield from self.channel.exchange_delete(exchange_name)

            self.assertEqual(cm.exception.code, 404)

        else:
            # weird result from rabbitmq 3.3.5
            result = yield from self.channel.exchange_delete(exchange_name)
            self.assertTrue(result)

class ExchangeBind(testcase.RabbitTestCase, unittest.TestCase):

    @testing.coroutine
    def test_exchange_bind(self):
        yield from self.channel.exchange_declare('exchange_destination', type_name='direct')
        yield from self.channel.exchange_declare('exchange_source', type_name='direct')

        result = yield from self.channel.exchange_bind(
            'exchange_destination', 'exchange_source', routing_key='')

        self.assertTrue(result)

    @testing.coroutine
    def test_inexistant_exchange_bind(self):
        with self.assertRaises(exceptions.ChannelClosed) as cm:
            result = yield from self.channel.exchange_bind(
                'exchange_destination', 'exchange_source', routing_key='')

        self.assertEqual(cm.exception.code, 404)


class ExchangeUnbind(testcase.RabbitTestCase, unittest.TestCase):


    @testing.coroutine
    def test_exchange_unbind(self):
        ex_source = 'exchange_source'
        ex_destination = 'exchange_destination'
        yield from self.channel.exchange_declare(ex_destination, type_name='direct')
        yield from self.channel.exchange_declare(ex_source, type_name='direct')

        yield from self.channel.exchange_bind(
            ex_destination, ex_source, routing_key='')

        result = yield from self.channel.exchange_unbind(
            ex_destination, ex_source, routing_key='')

    @testing.coroutine
    def test_exchange_unbind_reversed(self):
        ex_source = 'exchange_source'
        ex_destination = 'exchange_destination'
        yield from self.channel.exchange_declare(ex_destination, type_name='direct')
        yield from self.channel.exchange_declare(ex_source, type_name='direct')

        yield from self.channel.exchange_bind(
            ex_destination, ex_source, routing_key='')

        if self.server_version() < (3, 3, 5):
            with self.assertRaises(exceptions.ChannelClosed) as cm:
                result = yield from self.channel.exchange_unbind(
                    ex_source, ex_destination, routing_key='')

            self.assertEqual(cm.exception.code, 404)

        else:
            # weird result from rabbitmq 3.3.5
            result = yield from self.channel.exchange_unbind(ex_source, ex_destination, routing_key='')
            self.assertTrue(result)

