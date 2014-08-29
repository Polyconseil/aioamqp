import inspect
import logging
import os

import asyncio
from asyncio import subprocess

from . import testing
from .. import connect as aioamqp_connect
from .. import exceptions
from ..channel import Channel
from ..protocol import AmqpProtocol


logger = logging.getLogger(__name__)


class AmqpTestCase(testing.AsyncioTestCaseMixin):

    def setUp(self):
        super().setUp()
        self.virtualhost = "/aioamqptest"
        self.amqp = None
        self.channel = None

        self.channels = []
        self.queues = {}
        self.exchanges = {}
        self.connections = []

        self.loop.run_until_complete(self.connect())

    def tearDown(self):
        self.loop.run_until_complete(self._cleanup())
        super().tearDown()

    @asyncio.coroutine
    def _cleanup(self):
        for queue_name, queue in self.queues.items():
            try:
                yield from self.queue_delete(queue_name)
            except Exception:
                pass

        for channel in self.channels:
            if channel.is_open:
                yield from channel.close()

    @asyncio.coroutine
    def assertExchangeExists(self, exchange_name, type_name):
        try:
            yield from self.exchange_declare(exchange_name, type_name, passive=True)
        except exceptions.ChannelClosed:
            self.fail("Exchange {} does not exists".format(exchange_name))

    @asyncio.coroutine
    def connect(self):
        amqp = yield from aioamqp_connect(virtualhost=self.virtualhost)
        if self.amqp is None:
            self.amqp = amqp

        self.connections.append(amqp)
        return amqp

    @asyncio.coroutine
    def create_channel(self, amqp=None):
        if amqp is None:
            amqp = self.amqp
        channel = yield from amqp.channel()
        if not self.channel:
            self.channel = channel

        self.channels.append(channel)
        return channel

    @asyncio.coroutine
    def queue_declare(self, queue_name, *args, channel=None, **kw):
        channel = channel or self.channel
        # prefix queue_name with the test name
        try:
            rep = yield from channel.queue_declare(queue_name, *args, **kw)
        finally:
            self.queues[queue_name] = (queue_name, channel)
        return rep

    @asyncio.coroutine
    def exchange_declare(self, exchange_name, *args, channel=None, **kw):
        channel = channel or self.channel

        # prefix exchange name
        try:
            rep = yield from channel.exchange_declare(exchange_name, *args, **kw)
        finally:
            self.exchanges[exchange_name] = (exchange_name, channel)
        return rep

    def register_channel(self, channel):
        if channel not in self.channels:
            self.channels.append(channel)
