"""
    Test our Protocol class
"""

import asyncio
import unittest
import unittest.mock

from .. import connect as amqp_connect


class ProtocolTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()

    def test_connect(self):
        proto = self.loop.run_until_complete(amqp_connect())
        self.assertTrue(proto.is_connected)

    def test_connection_unexistant_vhost(self):
        proto = self.loop.run_until_complete(amqp_connect(virtualhost='/unexistant'))
        self.assertTrue(proto.is_connected)

    def test_connection_wrong_login_password(self):
        proto = self.loop.run_until_complete(amqp_connect(login='wrong', password='wrong'))
        proto = self.loop.run_until_complete(amqp_connect(virtualhost='/unexistant'))
        self.assertTrue(proto.is_connected)
