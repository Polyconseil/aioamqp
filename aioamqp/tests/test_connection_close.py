import unittest

import asyncio

from . import testcase


class CloseTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    def test_close(self):
        @asyncio.coroutine
        def go():
            amqp = self.amqp
            self.assertTrue(amqp.is_open)
            amqp.close()
            yield from asyncio.wait_for(amqp.wait_closed(), timeout=2)
            self.assertFalse(amqp.is_open)
        self.loop.run_until_complete(go())

    def test_multiple_close(self):
        @asyncio.coroutine
        def go():
            amqp = self.amqp
            amqp.close()
            yield from asyncio.wait_for(amqp.wait_closed(), timeout=2)
            self.assertFalse(amqp.is_open)
            amqp.close()
            yield from asyncio.wait_for(amqp.wait_closed(), timeout=2)
        self.loop.run_until_complete(go())
