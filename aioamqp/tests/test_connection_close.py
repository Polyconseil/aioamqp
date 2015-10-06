import unittest

from . import testcase
from . import testing


class CloseTestCase(testcase.RabbitTestCase, unittest.TestCase):

    @testing.coroutine
    def test_close(self):
        amqp = self.amqp
        self.assertTrue(amqp.is_open)
        yield from amqp.close()
        self.assertFalse(amqp.is_open)

    @testing.coroutine
    def test_multiple_close(self):
        amqp = self.amqp
        yield from amqp.close()
        self.assertFalse(amqp.is_open)
        yield from amqp.close()
