import logging
import os

import asyncio
from asyncio import subprocess

from .. import connect as aioamqp_connect


logger = logging.getLogger(__name__)


class RabbitTestCase:
    """TestCase with a rabbit running in background"""

    RABBIT_TIMEOUT = 1.0

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
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
                logger.debug('Delete queue %s', self.full_queue_name(queue_name))
                yield from self.safe_queue_delete(queue_name, channel)
            for exchange_name, channel in self.exchanges.values():
                logger.debug('Delete exchange %s', self.full_exchange_name(exchange_name))
                yield from self.safe_exchange_delete(exchange_name, channel)
        self.loop.run_until_complete(go())
        # TODO channel.close is a coroutine...
        for channel in self.channels:
            logger.debug('Delete channel %s', channel)
            channel.close()
            del channel
        for amqp in self.amqps:
            logger.debug('Delete amqp %s', amqp)
            del amqp
        self.loop.close()

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
    def rabbitctl_list(self, command, info, vhost=None):
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
            datadict[data['name']] = data
        return datadict

    @asyncio.coroutine
    def list_queues(self, vhost=None):
        info = ['name', 'durable', 'auto_delete',
            'arguments', 'policy', 'pid', 'owner_pid', 'exclusive_consumer_pid',
            'exclusive_consumer_tag', 'messages_ready', 'messages_unacknowledged', 'messages',
            'consumers', 'memory', 'slave_pids', 'synchronised_slave_pids']
        return (yield from self.rabbitctl_list('list_queues', info, vhost=vhost))

    @asyncio.coroutine
    def list_exchanges(self, vhost=None):
        info = ['name', 'type', 'durable', 'auto_delete', 'internal', 'arguments', 'policy']
        return (yield from self.rabbitctl_list('list_exchanges', info, vhost=vhost))

    @asyncio.coroutine
    def safe_queue_delete(self, queue_name, channel=None):
        """Delete the queue but does not raise any exception if it fails

        The operation has a timeout as well.
        """
        channel = channel or self.channel
        full_queue_name = self.full_queue_name(queue_name)
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
        full_exchange_name = self.full_exchange_name(exchange_name)
        try:
            yield from channel.exchange_delete(full_exchange_name, no_wait=False, timeout=1.0)
        except asyncio.TimeoutError:
            logger.warning('Timeout on exchange %s deletion', full_exchange_name, exc_info=True)
        except Exception:
            logger.error('Unexpected error on exchange %s deletion', full_exchange_name, exc_info=True)

    def full_queue_name(self, name):
        return self.id() + '.' + name

    def full_exchange_name(self, name):
        return self.id() + '.' + name

    @asyncio.coroutine
    def queue_declare(self, queue_name, *args, channel=None, safe_delete_before=True, **kw):
        channel = channel or self.channel
        if safe_delete_before:
            yield from self.safe_queue_delete(queue_name, channel=channel)
        # prefix queue_name with the test name
        full_queue_name = self.full_queue_name(queue_name)
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
        full_exchange_name = self.full_exchange_name(exchange_name)
        try:
            rep = yield from channel.exchange_declare(full_exchange_name, *args, **kw)
        finally:
            self.exchanges[exchange_name] = (exchange_name, channel)
        return rep

    @asyncio.coroutine
    def create_channel(self, amqp=None):
        amqp = amqp or self.amqp
        channel = yield from amqp.channel()
        self.channels.append(channel)
        return channel

    @asyncio.coroutine
    def create_amqp(self, vhost=None):
        vhost = vhost or self.vhost
        amqp = yield from aioamqp_connect(host=self.host, port=self.port, virtualhost=vhost)
        self.amqps.append(amqp)
        return amqp
