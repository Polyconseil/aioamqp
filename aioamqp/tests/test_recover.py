"""
    Amqp basic tests for recover methods
"""

import unittest

from . import testcase
from . import testing


class RecoverTestCase(testcase.RabbitTestCase, unittest.TestCase):

    async def test_basic_recover_async(self):
        await self.channel.basic_recover_async(requeue=True)

    async def test_basic_recover_async_no_requeue(self):
        await self.channel.basic_recover_async(requeue=False)

    async def test_basic_recover(self):
        result = await self.channel.basic_recover(requeue=True)
        self.assertTrue(result)
