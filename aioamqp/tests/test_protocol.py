"""
    Test our Protocol class
"""

import asyncio
import unittest
import unittest.mock

from .. import exceptions
from .. import connect as amqp_connect


class ProtocolTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        super().setUp()

    def tearDown(self):
        self.loop.close()
        super().tearDown()

    def test_connect(self):
        proto = self.loop.run_until_complete(amqp_connect())
        self.assertTrue(proto.is_connected)

    def test_connection_unexistant_vhost(self):
        with self.assertRaises(exceptions.ClosedConnection):
            self.loop.run_until_complete(amqp_connect(virtualhost='/unexistant'))

    def test_connection_wrong_login_password(self):
        with self.assertRaises(exceptions.ClosedConnection):
            self.loop.run_until_complete(amqp_connect(login='wrong', password='wrong'))
