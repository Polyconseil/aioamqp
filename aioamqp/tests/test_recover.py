"""
    Amqp basic tests for recover methods
"""

import unittest

from . import testcase
from . import testing


class RecoverTestCase(testcase.RabbitTestCase, unittest.TestCase):

    @testing.coroutine
    def test_basic_recover_async(self):
        yield from self.channel.basic_recover_async(requeue=True)

    @testing.coroutine
    def test_basic_recover_async_no_requeue(self):
        yield from self.channel.basic_recover_async(requeue=False)

    @testing.coroutine
    def test_basic_recover(self):
        result = yield from self.channel.basic_recover(requeue=True)
        self.assertTrue(result)
