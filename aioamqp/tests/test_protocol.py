"""
    Test our Protocol class
"""
import asynctest
from unittest import mock

from . import testcase
from .. import exceptions
from .. import connect as amqp_connect
from .. import from_url as amqp_from_url
from ..protocol import AmqpProtocol, OPEN


class ProtocolTestCase(testcase.RabbitTestCaseMixin, asynctest.TestCase):

    async def test_connect(self):
        _transport, protocol = await amqp_connect(
            host=self.host, port=self.port, virtualhost=self.vhost, loop=self.loop
        )
        self.assertEqual(protocol.state, OPEN)
        await protocol.close()

    async def test_connect_products_info(self):
        client_properties = {
            'program': 'aioamqp-tests',
            'program_version': '0.1.1',
        }
        _transport, protocol = await amqp_connect(
            host=self.host,
            port=self.port,
            virtualhost=self.vhost,
            client_properties=client_properties,
            loop=self.loop,
        )

        self.assertEqual(protocol.client_properties, client_properties)
        await protocol.close()

    async def test_connection_unexistant_vhost(self):
        with self.assertRaises(exceptions.AmqpClosedConnection):
            await amqp_connect(host=self.host, port=self.port, virtualhost='/unexistant', loop=self.loop)

    def test_connection_wrong_login_password(self):
        with self.assertRaises(exceptions.AmqpClosedConnection):
            self.loop.run_until_complete(
                amqp_connect(host=self.host, port=self.port, login='wrong', password='wrong', loop=self.loop)
            )

    async def test_connection_from_url(self):
        with mock.patch('aioamqp.connect') as connect:
            async def func(*x, **y):
                return 1, 2
            connect.side_effect = func
            await amqp_from_url('amqp://tom:pass@example.com:7777/myvhost', loop=self.loop)
            connect.assert_called_once_with(
                insist=False,
                password='pass',
                login_method='PLAIN',
                login='tom',
                host='example.com',
                protocol_factory=AmqpProtocol,
                virtualhost='myvhost',
                port=7777,
                loop=self.loop,
            )

    async def test_ssl_context_connection_from_url(self):
        ssl_context = mock.Mock()
        with mock.patch('aioamqp.connect') as connect:
            async def func(*x, **y):
                return 1, 2
            connect.side_effect = func
            await amqp_from_url('amqps://tom:pass@example.com:7777/myvhost', loop=self.loop, ssl=ssl_context)
            connect.assert_called_once_with(
                insist=False,
                password='pass',
                login_method='PLAIN',
                ssl=ssl_context,
                login='tom',
                host='example.com',
                protocol_factory=AmqpProtocol,
                virtualhost='myvhost',
                port=7777,
                loop=self.loop,
            )

    async def test_from_url_raises_on_wrong_scheme(self):
        with self.assertRaises(ValueError):
            await amqp_from_url('invalid://')
