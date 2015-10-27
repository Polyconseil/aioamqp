"""
    Tests for message properties for basic deliver
"""

import asyncio
import unittest
import logging

from . import testcase
from . import testing


logger = logging.getLogger(__name__)


class ReplyTestCase(testcase.RabbitTestCase, unittest.TestCase):
    @asyncio.coroutine
    def _server(self, server_future, exchange_name, routing_key):
        """Consume messages and reply to them by publishing messages back to the client using 
        routing key set to the reply_to property"""
        server_queue_name = 'server_queue'
        yield from self.channel.queue_declare(server_queue_name, exclusive=False, no_wait=False)
        yield from self.channel.exchange_declare(exchange_name, type_name='direct')
        yield from self.channel.queue_bind(
            server_queue_name, exchange_name, routing_key=routing_key)
        @asyncio.coroutine
        def server_callback(channel, body, envelope, properties):
            logger.debug('Server received message')
            server_future.set_result((body, envelope, properties))
            publish_properties = {'correlation_id': properties.correlation_id}
            logger.debug('Replying to %r', properties.reply_to)
            yield from self.channel.publish(
                b'reply message', exchange_name, properties.reply_to, publish_properties)
            logger.debug('Server replied')
        yield from self.channel.basic_consume(server_callback, queue_name=server_queue_name)
        logger.debug('Server consuming messages')

    @asyncio.coroutine
    def _client(
            self, client_future, exchange_name, server_routing_key, correlation_id,
            client_routing_key):
        """Declare a queue, bind client_routing_key to it and publish a message to the server with
        the reply_to property set to that routing key"""
        client_queue_name = 'client_reply_queue'
        client_channel = yield from self.create_channel()
        yield from client_channel.queue_declare(
            client_queue_name, exclusive=True, no_wait=False)
        yield from client_channel.queue_bind(
            client_queue_name, exchange_name, routing_key=client_routing_key)
        @asyncio.coroutine
        def client_callback(channel, body, envelope, properties):
            logger.debug('Client received message')
            client_future.set_result((body, envelope, properties))
        yield from client_channel.basic_consume(client_callback, queue_name=client_queue_name)
        logger.debug('Client consuming messages')

        yield from client_channel.publish(
            b'client message',
            exchange_name,
            server_routing_key,
            {'correlation_id': correlation_id, 'reply_to': client_routing_key})
        logger.debug('Client published message')

    @testing.coroutine
    def test_reply_to(self):
        exchange_name = 'exchange_name'
        server_routing_key = 'reply_test'

        server_future = asyncio.Future(loop=self.loop)
        yield from self._server(server_future, exchange_name, server_routing_key)

        correlation_id = 'secret correlation id'
        client_routing_key = 'secret_client_key'

        client_future = asyncio.Future(loop=self.loop)
        yield from self._client(
            client_future, exchange_name, server_routing_key, correlation_id, client_routing_key)

        logger.debug('Waiting for server to receive message')
        server_body, server_envelope, server_properties = yield from server_future
        self.assertEqual(server_body, b'client message')
        self.assertEqual(server_properties.correlation_id, correlation_id)
        self.assertEqual(server_properties.reply_to, client_routing_key)
        self.assertEqual(server_envelope.routing_key, server_routing_key)

        logger.debug('Waiting for client to receive message')
        client_body, client_envelope, client_properties = yield from client_future
        self.assertEqual(client_body, b'reply message')
        self.assertEqual(client_properties.correlation_id, correlation_id)
        self.assertEqual(client_envelope.routing_key, client_routing_key)
