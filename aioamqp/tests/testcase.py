import inspect
import logging
import os

import asyncio
from asyncio import subprocess

from . import testing
from .. import connect as aioamqp_connect
from ..channel import Channel
from ..protocol import AmqpProtocol


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
    exchange_bind = use_full_name(Channel.exchange_bind, ['exchange_source', 'exchange_destination'])
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

    def setUp(self):
        super().setUp()
        self.vhost = '/'
        self.host = 'localhost'
        self.port = 5672
        self.queues = {}
        self.exchanges = {}
        self.channels = []
        self.amqps = []

        @asyncio.coroutine
        def go():
            amqp = yield from self.create_amqp()
            self.amqps.append(amqp)
            channel = yield from self.create_channel()
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
        self.loop.run_until_complete(go())
        for channel in self.channels:
            logger.debug('Delete channel %s', channel)
            channel.close()
            del channel
        for amqp in self.amqps:
            logger.debug('Delete amqp %s', amqp)
            del amqp
        super().tearDown()

    @property
    def amqp(self):
        return self.amqps[0]

    @property
    def channel(self):
        return self.channels[0]

    def get_rabbitmqctl_exe(self):
        paths = [
            os.path.join(os.path.expanduser('~'), 'sbin/rabbitmqctl'),
            '/usr/local/sbin/rabbitmqctl',
            '/usr/sbin/rabbitmqctl',
        ]
        for path in paths:
            if os.path.exists(path):
                return path
        return 'rabbitmqctl'

    @asyncio.coroutine
    def rabbitctl(self, *args):
        exe_name = self.get_rabbitmqctl_exe()
        proc = yield from asyncio.create_subprocess_exec(
            exe_name, *args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        try:
            stdout, stderr = yield from proc.communicate()
        except:
            proc.kill()
            yield from proc.wait()
            raise
        if stdout is None:
            stdout = ''
        else:
            stdout = stdout.decode('utf8')
        if stderr is None:
            stderr = ''
        else:
            stderr = stderr.decode('utf8')
        exitcode = yield from proc.wait()
        if exitcode != 0 or stderr:
            raise ValueError(exitcode, stdout, stderr)
        return stdout

    @asyncio.coroutine
    def rabbitctl_list(self, command, info, vhost=None, fully_qualified_name=False):
        args = [command] + info
        if vhost is not None:
            args += ['-p', vhost]
        rep = yield from self.rabbitctl(*args)
        lines = rep.strip().split('\n')
        lines = lines[1:-1]
        lines = [line.split('\t') for line in lines]
        datadict = {}
        for datainfo in lines:
            data = {}
            for info_name, info_value in zip(info, datainfo):
                if info_value == 'true':
                    info_value = True
                elif info_value == 'false':
                    info_value = False
                else:
                    try:
                        info_value = int(info_value)
                    except ValueError:
                        try:
                            info_value = float(info_value)
                        except ValueError:
                            pass
                data[info_name] = info_value
            if not fully_qualified_name:
                data['name'] = self.local_name(data['name'])
            datadict[data['name']] = data
        return datadict

    @asyncio.coroutine
    def list_queues(self, vhost=None, fully_qualified_name=False):
        info = ['name', 'durable', 'auto_delete',
            'arguments', 'policy', 'pid', 'owner_pid', 'exclusive_consumer_pid',
            'exclusive_consumer_tag', 'messages_ready', 'messages_unacknowledged', 'messages',
            'consumers', 'memory', 'slave_pids', 'synchronised_slave_pids']
        return (yield from self.rabbitctl_list('list_queues', info, vhost=vhost,
            fully_qualified_name=fully_qualified_name))

    @asyncio.coroutine
    def list_exchanges(self, vhost=None, fully_qualified_name=False):
        info = ['name', 'type', 'durable', 'auto_delete', 'internal', 'arguments', 'policy']
        return (yield from self.rabbitctl_list('list_exchanges', info, vhost=vhost,
            fully_qualified_name=fully_qualified_name))

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
        except Exception:
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
        except Exception:
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
        amqp = yield from aioamqp_connect(host=self.host, port=self.port, virtualhost=vhost,
            protocol_factory=protocol_factory)
        self.amqps.append(amqp)
        return amqp
