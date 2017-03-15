"""Aioamqp tests utilities

Provides the test case to simplify testing
"""

import asyncio
import inspect
import logging
import os
import time
import uuid

import pyrabbit.api

from . import testing
from .. import connect as aioamqp_connect
from .. import exceptions
from ..channel import Channel
from ..protocol import AmqpProtocol, OPEN


logger = logging.getLogger(__name__)


def use_full_name(f, arg_names):
    sig = inspect.signature(f)
    for arg_name in arg_names:
        if arg_name not in sig.parameters:
            raise ValueError('%s is not a valid argument name for function %s' % (arg_name, f.__qualname__))

    def wrapper(self, *args, **kw):
        ba = sig.bind_partial(self, *args, **kw)
        for param in sig.parameters.values():
            if param.name in arg_names and param.name in ba.arguments:
                ba.arguments[param.name] = self.full_name(ba.arguments[param.name])
        return f(*(ba.args), **(ba.kwargs))

    return wrapper


class ProxyChannel(Channel):
    def __init__(self, test_case, *args, **kw):
        super().__init__(*args, **kw)
        self.test_case = test_case
        self.test_case.register_channel(self)

    exchange_declare = use_full_name(Channel.exchange_declare, ['exchange_name'])
    exchange_delete = use_full_name(Channel.exchange_delete, ['exchange_name'])
    queue_declare = use_full_name(Channel.queue_declare, ['queue_name'])
    queue_delete = use_full_name(Channel.queue_delete, ['queue_name'])
    queue_bind = use_full_name(Channel.queue_bind, ['queue_name', 'exchange_name'])
    queue_unbind = use_full_name(Channel.queue_unbind, ['queue_name', 'exchange_name'])
    queue_purge = use_full_name(Channel.queue_purge, ['queue_name'])

    exchange_bind = use_full_name(Channel.exchange_bind, ['exchange_source', 'exchange_destination'])
    exchange_unbind = use_full_name(Channel.exchange_unbind, ['exchange_source', 'exchange_destination'])
    publish = use_full_name(Channel.publish, ['exchange_name'])
    basic_get = use_full_name(Channel.basic_get, ['queue_name'])
    basic_consume = use_full_name(Channel.basic_consume, ['queue_name'])

    def full_name(self, name):
        return self.test_case.full_name(name)


class ProxyAmqpProtocol(AmqpProtocol):
    def __init__(self, test_case, *args, **kw):
        super().__init__(*args, **kw)
        self.test_case = test_case

    def channel_factory(self, protocol, channel_id):
        return ProxyChannel(self.test_case, protocol, channel_id)
    CHANNEL_FACTORY = channel_factory


class RabbitTestCase(testing.AsyncioTestCaseMixin):
    """TestCase with a rabbit running in background"""

    RABBIT_TIMEOUT = 1.0
    VHOST = 'test-aioamqp'

    def setUp(self):
        super().setUp()
        self.host = os.environ.get('AMQP_HOST', 'localhost')
        self.port = os.environ.get('AMQP_PORT', 5672)
        self.vhost = os.environ.get('AMQP_VHOST', self.VHOST + str(uuid.uuid4()))
        self.http_client = pyrabbit.api.Client('localhost:15672/api/', 'guest', 'guest')

        self.amqps = []
        self.channels = []
        self.exchanges = {}
        self.queues = {}
        self.transports = []

        self.reset_vhost()

    def reset_vhost(self):
        try:
            self.http_client.delete_vhost(self.vhost)
        except Exception:  # pylint: disable=broad-except
            pass

        self.http_client.create_vhost(self.vhost)
        self.http_client.set_vhost_permissions(
            vname=self.vhost, username='guest', config='.*', rd='.*', wr='.*',
        )

        @asyncio.coroutine
        def go():
            _transport, protocol = yield from self.create_amqp()
            channel = yield from self.create_channel(amqp=protocol)
            self.channels.append(channel)
        self.loop.run_until_complete(go())

    def tearDown(self):
        @asyncio.coroutine
        def go():
            for queue_name, channel in self.queues.values():
                logger.debug('Delete queue %s', self.full_name(queue_name))
                yield from self.safe_queue_delete(queue_name, channel)
            for exchange_name, channel in self.exchanges.values():
                logger.debug('Delete exchange %s', self.full_name(exchange_name))
                yield from self.safe_exchange_delete(exchange_name, channel)
            for amqp in self.amqps:
                if amqp.state != OPEN:
                    continue
                logger.debug('Delete amqp %s', amqp)
                yield from amqp.close()
                del amqp
        self.loop.run_until_complete(go())

        try:
            self.http_client.delete_vhost(self.vhost)
        except Exception:  # pylint: disable=broad-except
            pass

        super().tearDown()

    @property
    def amqp(self):
        return self.amqps[0]

    @property
    def channel(self):
        return self.channels[0]

    def server_version(self, amqp=None):
        if amqp is None:
            amqp = self.amqp

        server_version = tuple(int(x) for x in amqp.server_properties['version'].split('.'))
        return server_version

    @asyncio.coroutine
    def check_exchange_exists(self, exchange_name):
        """Check if the exchange exist"""
        try:
            yield from self.exchange_declare(exchange_name, passive=True)
        except exceptions.ChannelClosed:
            return False

        return True

    @asyncio.coroutine
    def assertExchangeExists(self, exchange_name):
        if not self.check_exchange_exists(exchange_name):
            self.fail("Exchange {} does not exists".format(exchange_name))

    @asyncio.coroutine
    def check_queue_exists(self, queue_name):
        """Check if the queue exist"""
        try:
            yield from self.queue_declare(queue_name, passive=True)
        except exceptions.ChannelClosed:
            return False

        return True

    @asyncio.coroutine
    def assertQueueExists(self, queue_name):
        if not self.check_queue_exists(queue_name):
            self.fail("Queue {} does not exists".format(queue_name))

    def list_queues(self, vhost=None, fully_qualified_name=False):
        # wait for the http client to get the correct state of the queue
        time.sleep(int(os.environ.get('AMQP_REFRESH_TIME', 3)))
        queues_list = self.http_client.get_queues(vhost=vhost or self.vhost)
        queues = {}
        for queue_info in queues_list:
            queue_name = queue_info['name']
            if fully_qualified_name is False:
                queue_name = self.local_name(queue_info['name'])
                queue_info['name'] = queue_name

            queues[queue_name] = queue_info
        return queues

    @asyncio.coroutine
    def safe_queue_delete(self, queue_name, channel=None):
        """Delete the queue but does not raise any exception if it fails

        The operation has a timeout as well.
        """
        channel = channel or self.channel
        full_queue_name = self.full_name(queue_name)
        try:
            yield from channel.queue_delete(full_queue_name, no_wait=False, timeout=1.0)
        except asyncio.TimeoutError:
            logger.warning('Timeout on queue %s deletion', full_queue_name, exc_info=True)
        except Exception:  # pylint: disable=broad-except
            logger.error('Unexpected error on queue %s deletion', full_queue_name, exc_info=True)

    @asyncio.coroutine
    def safe_exchange_delete(self, exchange_name, channel=None):
        """Delete the exchange but does not raise any exception if it fails

        The operation has a timeout as well.
        """
        channel = channel or self.channel
        full_exchange_name = self.full_name(exchange_name)
        try:
            yield from channel.exchange_delete(full_exchange_name, no_wait=False, timeout=1.0)
        except asyncio.TimeoutError:
            logger.warning('Timeout on exchange %s deletion', full_exchange_name, exc_info=True)
        except Exception:  # pylint: disable=broad-except
            logger.error('Unexpected error on exchange %s deletion', full_exchange_name, exc_info=True)

    def full_name(self, name):
        if self.is_full_name(name):
            return name
        else:
            return self.id() + '.' + name

    def local_name(self, name):
        if self.is_full_name(name):
            return name[len(self.id()) + 1:]  # +1 because of the '.'
        else:
            return name

    def is_full_name(self, name):
        return name.startswith(self.id())

    @asyncio.coroutine
    def queue_declare(self, queue_name, *args, channel=None, safe_delete_before=True, **kw):
        channel = channel or self.channel
        if safe_delete_before:
            yield from self.safe_queue_delete(queue_name, channel=channel)
        # prefix queue_name with the test name
        full_queue_name = self.full_name(queue_name)
        try:
            rep = yield from channel.queue_declare(full_queue_name, *args, **kw)

        finally:
            self.queues[queue_name] = (queue_name, channel)
        return rep

    @asyncio.coroutine
    def exchange_declare(self, exchange_name, *args, channel=None, safe_delete_before=True, **kw):
        channel = channel or self.channel
        if safe_delete_before:
            yield from self.safe_exchange_delete(exchange_name, channel=channel)
        # prefix exchange name
        full_exchange_name = self.full_name(exchange_name)
        try:
            rep = yield from channel.exchange_declare(full_exchange_name, *args, **kw)
        finally:
            self.exchanges[exchange_name] = (exchange_name, channel)
        return rep

    def register_channel(self, channel):
        self.channels.append(channel)

    @asyncio.coroutine
    def create_channel(self, amqp=None):
        amqp = amqp or self.amqp
        channel = yield from amqp.channel()
        return channel

    @asyncio.coroutine
    def create_amqp(self, vhost=None):
        def protocol_factory(*args, **kw):
            return ProxyAmqpProtocol(self, *args, **kw)
        vhost = vhost or self.vhost
        transport, protocol = yield from aioamqp_connect(host=self.host, port=self.port, virtualhost=vhost,
            protocol_factory=protocol_factory, loop=self.loop)
        self.amqps.append(protocol)
        return transport, protocol
