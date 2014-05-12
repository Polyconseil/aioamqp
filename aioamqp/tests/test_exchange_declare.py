import logging
import unittest

import asyncio

from . import testcase
from .. import exceptions


logger = logging.getLogger(__name__)


class ExchangeDeclareTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @asyncio.coroutine
    def _test_exchange_declare(self, exchange_name, type_name, durable=False, auto_delete=False):
        # declare exchange
        yield from self.exchange_declare(exchange_name, type_name, no_wait=False,
            durable=durable, auto_delete=auto_delete, timeout=self.RABBIT_TIMEOUT)

        # retrieve exchange info from rabbitmqctl
        exchanges = yield from self.list_exchanges()
        exchange = exchanges[exchange_name]

        # assert exchange has been declared witht the good arguments
        self.assertEqual(exchange_name, exchange['name'])
        self.assertEqual(type_name, exchange['type'])
        self.assertEqual(auto_delete, exchange['auto_delete'])
        self.assertEqual(durable, exchange['durable'])

        # delete exchange
        yield from self.safe_exchange_delete(exchange_name)

    def test_durable_and_auto_deleted(self):
        self.loop.run_until_complete(self._test_exchange_declare('e', 'direct',
            durable=True, auto_delete=True))

    def test_durable_and_not_auto_deleted(self):
        self.loop.run_until_complete(self._test_exchange_declare('e', 'direct',
            durable=True, auto_delete=False))

    def test_not_durable_and_auto_deleted(self):
        self.loop.run_until_complete(self._test_exchange_declare('e', 'direct',
            durable=False, auto_delete=True))

    def test_not_durable_and_not_auto_deleted(self):
        self.loop.run_until_complete(self._test_exchange_declare('e', 'direct',
            durable=False, auto_delete=False))

    def test_passive(self):
        @asyncio.coroutine
        def go():
            yield from self.safe_exchange_delete('e')
            # ask for non-existing exchange
            channel = yield from self.create_channel()
            with self.assertRaises(exceptions.ChannelClosed):
                yield from channel.exchange_declare("e", 'direct', passive=True)
            # create exchange
            yield from self.exchange_declare('e', 'direct')
            # get info
            channel = yield from self.create_channel()
            frame = yield from channel.exchange_declare("e", 'direct', passive=True)
            self.assertIsNotNone(frame)
        self.loop.run_until_complete(go())
