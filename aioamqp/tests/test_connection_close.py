import unittest

import asyncio

from . import testcase
from . import testing


class CloseTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_close(self):
        amqp = self.amqp
        self.assertTrue(amqp.is_open)
        amqp.close()
        yield from amqp.wait_closed()
        self.assertFalse(amqp.is_open)

    @testing.coroutine
    def test_multiple_close(self):
        amqp = self.amqp
        amqp.close()
        yield from amqp.wait_closed()
        self.assertFalse(amqp.is_open)
        amqp.close()
        yield from amqp.wait_closed()
