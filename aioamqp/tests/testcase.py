import logging

import asyncio
from asyncio import subprocess

from .. import connect as aioamqp_connect


logger = logging.getLogger(__name__)


class RabbitTestCase:
    """TestCase with a rabbit running in background"""

    RABBIT_TIMEOUT = 1.0

    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.vhost = '/'
        self.host = 'localhost'
        self.port = 5672
        self.queues = {}
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
        self.loop.run_until_complete(go())
        for channel in self.channels:
            logger.debug('Delete channel %s', channel)
            channel.close()
            del channel
        for amqp in self.amqps:
            logger.debug('Delete amqp %s', amqp)
            del amqp

    @property
    def amqp(self):
        return self.amqps[0]

    @property
    def channel(self):
        return self.channels[0]

    @asyncio.coroutine
    def rabbitctl(self, *args):
        proc = yield from asyncio.create_subprocess_exec(
            'rabbitmqctl', *args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        try:
            stdout, stderr = yield from proc.communicate()
        except:
            proc.kill()
            yield from proc.wait()
            raise
        exitcode = yield from proc.wait()
        if exitcode != 0 or stderr:
            raise ValueError(exitcode, stderr.decode('utf8'))
        return stdout.decode('utf8')

    @asyncio.coroutine
    def rabbitctl_list(self, *args):
        rep = yield from self.rabbitctl(*args)
        lines = rep.strip().split('\n')
        lines = lines[1:-1]
        lines = [line.split('\t') for line in lines]
        return lines

    @asyncio.coroutine
    def list_queues(self, vhost=None):
        info = ['name', 'durable', 'auto_delete',
            'arguments', 'policy', 'pid', 'owner_pid', 'exclusive_consumer_pid',
            'exclusive_consumer_tag', 'messages_ready', 'messages_unacknowledged', 'messages',
            'consumers', 'memory', 'slave_pids', 'synchronised_slave_pids', 'status']
        args = ['list_queues'] + info
        if vhost is not None:
            args += ['-p', vhost]
        rep = yield from self.rabbitctl_list(*args)
        queues = {}
        for queueinfo in rep:
            queue = {}
            for info_name, info_value in zip(info, queueinfo):
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
                queue[info_name] = info_value
            queues[queue['name']] = queue
        return queues

    @asyncio.coroutine
    def safe_queue_delete(self, queue_name, channel=None):
        """Delete the queue but does not raise any exception if it fails

        The operation has a timeout as well.
        """
        channel = channel or self.channel
        full_queue_name = self.full_queue_name(queue_name)
        try:
            yield from channel.queue_delete(full_queue_name, no_wait=False, timeout=1.0)
        except asyncio.TimeoutError as ex:
            logger.warning('Timeout on queue %s deletion\n%s', full_queue_name, traceback.format_exc(ex))
        except Exception as ex:
            logger.error('Unexpected error on queue %s deletion\n%s', fulle_queue_name, traceback.format_exc(ex))

    def full_queue_name(self, queue_name):
        return self.id() + '.' + queue_name

    @asyncio.coroutine
    def queue_declare(self, queue_name, *args, channel=None, exclusive=True, safe_delete_before=True, **kw):
        channel = channel or self.channel
        if safe_delete_before:
            yield from self.safe_queue_delete(queue_name, channel=channel)
        # prefix queue_name with the test name
        full_queue_name = self.full_queue_name(queue_name)
        try:
            rep = yield from channel.queue_declare(full_queue_name, *args, exclusive=exclusive, **kw)
        finally:
            self.queues[queue_name] = (queue_name, channel)
        rep = rep or queue_name
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
        amqp = yield from aioamqp_connect(host=self.host, port=self.port)
        yield from amqp.start_connection(virtual_host=vhost)
        self.amqps.append(amqp)
        return amqp
