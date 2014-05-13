"""
    Test our Protocol class
"""

import asyncio
import unittest
from unittest import mock

from .. import exceptions
from .. import connect as amqp_connect
from .. import from_url as amqp_from_url
from .. import protocol


class ProtocolTestCase(unittest.TestCase):

    _multiprocess_can_split_ = True

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        super().setUp()

    def tearDown(self):
        self.loop.close()
        super().tearDown()

    def test_connect(self):
        proto = self.loop.run_until_complete(amqp_connect())
        self.assertTrue(proto.is_open)

    def test_connection_unexistant_vhost(self):
        with self.assertRaises(exceptions.AmqpClosedConnection):
            self.loop.run_until_complete(amqp_connect(virtualhost='/unexistant'))

    def test_connection_wrong_login_password(self):
        with self.assertRaises(exceptions.AmqpClosedConnection):
            self.loop.run_until_complete(amqp_connect(login='wrong', password='wrong'))

    def test_connection_from_url(self):
        @asyncio.coroutine
        def go():
            with mock.patch('aioamqp.connect') as connect:
                yield from amqp_from_url('amqp://tom:pass@chev.eu:7777/myvhost')
                connect.assert_called_once_with(
                    insist=False,
                    password='pass',
                    login_method='AMQPLAIN',
                    ssl=False,
                    login='tom',
                    host='chev.eu',
                    protocol_factory=protocol.AmqpProtocol,
                    virtualhost='/myvhost',
                    port=7777
                )
        self.loop.run_until_complete(go())

    def test_from_url_raises_on_wrong_scheme(self):
        with self.assertRaises(ValueError):
            self.loop.run_until_complete(amqp_from_url('invalid://'))
