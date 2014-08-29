import logging
import unittest

import asyncio

from . import testcase
from . import testing
from .. import exceptions


logger = logging.getLogger(__name__)


class ExchangeDeclareTestCase(testcase.AmqpTestCase, unittest.TestCase):


    def _test_exchange_declare(self, exchange_name, type_name, durable=False, auto_delete=False):
        yield from self.create_channel()
        # declare exchange
        yield from self.exchange_declare(exchange_name, type_name, no_wait=False,
            durable=durable, auto_delete=auto_delete)

        yield from self.assertExchangeExists(exchange_name, type_name)

    def test_durable_and_auto_deleted(self):
        exchange_name = "test_durable_and_auto_deleted"
        self.loop.run_until_complete(self._test_exchange_declare(exchange_name, 'direct',
            durable=True, auto_delete=True))

    def test_durable_and_not_auto_deleted(self):
        exchange_name = "test_durable_and_not_auto_deleted"
        self.loop.run_until_complete(self._test_exchange_declare(exchange_name, 'direct',
            durable=True, auto_delete=False))

    def test_not_durable_and_auto_deleted(self):
        exchange_name = "test_not_durable_and_auto_deleted"
        self.loop.run_until_complete(self._test_exchange_declare(exchange_name, 'direct',
            durable=False, auto_delete=True))

    def test_not_durable_and_not_auto_deleted(self):
        exchange_name = "test_not_durable_and_not_auto_deleted"
        self.loop.run_until_complete(self._test_exchange_declare(exchange_name, 'direct',
            durable=False, auto_delete=False))

    def test_passive(self):
        exchange_name = "test_passive"
        # ask for non-existing exchange
        channel = yield from self.create_channel()
        with self.assertRaises(exceptions.ChannelClosed):
            yield from channel.exchange_declare(exchange_name, 'direct', passive=True)
        # create exchange
        yield from self.exchange_declare(exchange_name, 'direct')
        # get info
        channel = yield from self.create_channel()
        frame = yield from channel.exchange_declare(exchange_name, 'direct', passive=True)
        self.assertIsNotNone(frame)
