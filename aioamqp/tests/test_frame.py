"""
    Test frame format.
"""

import io
import unittest
import sys
import datetime

from decimal import Decimal

from .. import constants as amqp_constants
from .. import frame as frame_module
from ..frame import AmqpResponse


class AmqpResponseTestCase(unittest.TestCase):
    def test_dump_dont_crash(self):
        frame = AmqpResponse(None)
        frame.frame_type = amqp_constants.TYPE_METHOD
        frame.class_id = 0
        frame.method_id = 0
        saved_stout = sys.stdout
        frame_module.DUMP_FRAMES = True
        sys.stdout = io.StringIO()
        try:
            last_len = len(sys.stdout.getvalue())
            print(self)
            # assert something has been writen
            self.assertLess(last_len, len(sys.stdout.getvalue()))
        finally:
            frame_module.DUMP_FRAMES = False
            sys.stdout = saved_stout
