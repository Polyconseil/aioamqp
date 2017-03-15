"""
    Test our Protocol class
"""

import asyncio
import unittest
from unittest import mock

from . import testing
from . import testcase
from .. import exceptions
from .. import connect as amqp_connect
from .. import from_url as amqp_from_url
from ..protocol import AmqpProtocol, OPEN


class ProtocolTestCase(testcase.RabbitTestCase, unittest.TestCase):


    @testing.coroutine
    def test_connect(self):
        _transport, protocol = yield from amqp_connect(virtualhost=self.vhost, loop=self.loop)
        self.assertEqual(protocol.state, OPEN)
        yield from protocol.close()

    @testing.coroutine
    def test_connect_products_info(self):
        client_properties = {
            'program': 'aioamqp-tests',
            'program_version': '0.1.1',
        }
        _transport, protocol = yield from amqp_connect(
            virtualhost=self.vhost,
            client_properties=client_properties,
            loop=self.loop,
        )

        self.assertEqual(protocol.client_properties, client_properties)
        yield from protocol.close()

    @testing.coroutine
    def test_connection_unexistant_vhost(self):
        with self.assertRaises(exceptions.AmqpClosedConnection):
            yield from amqp_connect(virtualhost='/unexistant', loop=self.loop)

    def test_connection_wrong_login_password(self):
        with self.assertRaises(exceptions.AmqpClosedConnection):
            self.loop.run_until_complete(amqp_connect(login='wrong', password='wrong', loop=self.loop))

    @testing.coroutine
    def test_connection_from_url(self):
        with mock.patch('aioamqp.connect') as connect:
            @asyncio.coroutine
            def func(*x, **y):
                return 1, 2
            connect.side_effect = func
            yield from amqp_from_url('amqp://tom:pass@example.com:7777/myvhost', loop=self.loop)
            connect.assert_called_once_with(
                insist=False,
                password='pass',
                login_method='AMQPLAIN',
                ssl=False,
                login='tom',
                host='example.com',
                protocol_factory=AmqpProtocol,
                virtualhost='myvhost',
                port=7777,
                verify_ssl=True,
                loop=self.loop,
            )

    @testing.coroutine
    def test_from_url_raises_on_wrong_scheme(self):
        with self.assertRaises(ValueError):
            yield from amqp_from_url('invalid://')
