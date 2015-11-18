"""
    Test our Protocol class
"""

import asyncio
import unittest
from unittest import mock

from . import testing
from .. import exceptions
from .. import connect as amqp_connect
from .. import from_url as amqp_from_url
from .. import protocol


class ProtocolTestCase(unittest.TestCase, testing.AsyncioTestCaseMixin):

    _multiprocess_can_split_ = True

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        super().setUp()

    def tearDown(self):
        self.loop.close()
        super().tearDown()

    def test_connect(self):
        transport, protocol = self.loop.run_until_complete(amqp_connect())
        self.assertTrue(protocol.is_open)

    def test_connect_products_info(self):
        transport, protocol = self.loop.run_until_complete(amqp_connect(
            product='test_product',
            product_version='0.1.0',
        ))

        self.assertEqual(protocol.product, 'test_product')
        self.assertEqual(protocol.product_version, '0.1.0')

    def test_connection_unexistant_vhost(self):
        with self.assertRaises(exceptions.AmqpClosedConnection):
            self.loop.run_until_complete(amqp_connect(virtualhost='/unexistant'))

    def test_connection_wrong_login_password(self):
        with self.assertRaises(exceptions.AmqpClosedConnection):
            self.loop.run_until_complete(amqp_connect(login='wrong', password='wrong'))

    @testing.coroutine
    def test_connection_from_url(self):
        with mock.patch('aioamqp.connect') as connect:
            @asyncio.coroutine
            def func(*x,**y):
                return 1,2
            connect.side_effect = func
            yield from amqp_from_url('amqp://tom:pass@example.com:7777/myvhost')
            connect.assert_called_once_with(
                insist=False,
                password='pass',
                login_method='AMQPLAIN',
                ssl=False,
                login='tom',
                host='example.com',
                protocol_factory=protocol.AmqpProtocol,
                virtualhost='myvhost',
                port=7777,
                verify_ssl=True,
            )

    @testing.coroutine
    def test_from_url_raises_on_wrong_scheme(self):
        with self.assertRaises(ValueError):
            yield from amqp_from_url('invalid://')
