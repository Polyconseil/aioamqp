"""Aioamqp tests"""

import unittest

from . import testing, testcase

from aioamqp import connect


class AmqpConnectionTestCase(testcase.RabbitTestCase, unittest.TestCase):

    @testing.coroutine
    def test_connect(self):
        transport, proto = yield from connect(vhost=self.vhost, loop=self.loop)
        self.assertTrue(proto.is_open)
        self.assertIsNotNone(proto.server_properties)


