"""
    Server received requests handling tests

"""

import unittest

from . import testcase
from . import testing


class ServerBasicCancelTestCase(testcase.RabbitTestCase, unittest.TestCase):
    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_cancel_whilst_consuming(self):
        queue_name = 'queue_name'
        yield from self.channel.queue_declare(queue_name)

        # None is non-callable.  We want to make sure the callback is
        # unregistered and never called.
        yield from self.channel.basic_consume(None)
        yield from self.channel.queue_delete(queue_name)
