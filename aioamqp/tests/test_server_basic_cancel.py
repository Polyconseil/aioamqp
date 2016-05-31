"""
    Server received requests handling tests

"""

import asyncio
import unittest

from . import testcase
from . import testing
from .. import exceptions


class ServerBasicCancelTestCase(testcase.RabbitTestCase, unittest.TestCase):
    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_cancel_whilst_consuming(self):
        queue_name = 'queue_name'
        yield from self.channel.queue_declare(queue_name)

        yield from self.channel.basic_consume(None)
        yield from self.channel.queue_delete(queue_name)
        self.assertEqual(self.amqps[0].dispatch_errors, [])
