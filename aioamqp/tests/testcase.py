import asyncio
from asyncio import subprocess

from .. import connect as aioamqp_connect


class RabbitTestCase:
    """TestCase with a rabbit running in background"""

    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.vhost = '/'
        self.host = 'localhost'
        self.port = 5672
        @asyncio.coroutine
        def go():
            self.amqp = yield from aioamqp_connect(host=self.host, port=self.port)
            yield from self.amqp.start_connection(virtual_host=self.vhost)
        self.loop.run_until_complete(go())

    def tearDown(self):
        del self.amqp

    def queue_name(self, identifier):
        return self.__module__ + '.' + self.__class__.__qualname__ + '.' + identifier

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


class RabbitWithChannelTestCase(RabbitTestCase):
    """TestCase with a rabbit and a pre opened channe"""

    def setUp(self):
        super().setUp()
        @asyncio.coroutine
        def go():
            self.channel = yield from self.amqp.channel()
        self.loop.run_until_complete(go())

    def tearDown(self):
        self.channel.close()
        del self.channel
        super().tearDown()
