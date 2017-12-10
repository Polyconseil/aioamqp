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
from ..frame import AmqpEncoder
from ..frame import AmqpResponse


class EncoderTestCase(unittest.TestCase):
    """Test encoding of python builtin objects to AMQP frames."""

    _multiprocess_can_split_ = True

    def setUp(self):
        self.encoder = AmqpEncoder()

    def test_write_string(self):
        self.encoder.write_value("foo")
        self.assertEqual(self.encoder.payload.getvalue(),
                         # 'S' + size (4 bytes) + payload
                         b'S\x00\x00\x00\x03foo')

    def test_write_bool(self):
        self.encoder.write_value(True)
        self.assertEqual(self.encoder.payload.getvalue(), b't\x01')

    def test_write_array(self):
        self.encoder.write_value(["v1", 123])
        self.assertEqual(self.encoder.payload.getvalue(),
                         # total size (4 bytes) + 'S' + size (4 bytes) + payload + 'I' + size (4 bytes) + payload
                         b'A\x00\x00\x00\x0cS\x00\x00\x00\x02v1I\x00\x00\x00{')

    def test_write_float(self):
        self.encoder.write_value(1.1)
        self.assertEqual(self.encoder.payload.getvalue(), b'd?\xf1\x99\x99\x99\x99\x99\x9a')

    def test_write_decimal(self):
        self.encoder.write_value(Decimal("-1.1"))
        self.assertEqual(self.encoder.payload.getvalue(), b'D\x01\xff\xff\xff\xf5')

        self.encoder.write_value(Decimal("1.1"))
        self.assertEqual(self.encoder.payload.getvalue(), b'D\x01\xff\xff\xff\xf5D\x01\x00\x00\x00\x0b')

    def test_write_datetime(self):
        self.encoder.write_value(datetime.datetime(2017, 12, 10, 4, 6, 49, 548918))
        self.assertEqual(self.encoder.payload.getvalue(), b'T\x00\x00\x00\x00Z,\xb2\xd9')

    def test_write_dict(self):
        self.encoder.write_value({'foo': 'bar', 'bar': 'baz'})
        self.assertIn(self.encoder.payload.getvalue(),
            # 'F' + total size + key (always a string) + value (with type) + ...
            # The keys are not ordered, so the output is not deterministic (two possible values below)
            (b'F\x00\x00\x00\x18\x03barS\x00\x00\x00\x03baz\x03fooS\x00\x00\x00\x03bar',
             b'F\x00\x00\x00\x18\x03fooS\x00\x00\x00\x03bar\x03barS\x00\x00\x00\x03baz'))

    def test_write_none(self):
        self.encoder.write_value(None)
        self.assertEqual(self.encoder.payload.getvalue(), b'V')

    def test_write_message_properties_dont_crash(self):
        properties = {
            'content_type': 'plain/text',
            'content_encoding': 'utf8',
            'headers': {'key': 'value'},
            'delivery_mode': 2,
            'priority': 10,
            'correlation_id': '122',
            'reply_to': 'joe',
            'expiration': 'someday',
            'message_id': 'm_id',
            'timestamp': 12345,
            'type': 'a_type',
            'user_id': 'joe_42',
            'app_id': 'roxxor_app',
            'cluster_id': 'a_cluster',
        }
        self.encoder.write_message_properties(properties)
        self.assertNotEqual(0, len(self.encoder.payload.getvalue()))

    def test_write_message_correlation_id_encode(self):
        properties = {
            'delivery_mode': 2,
            'priority': 0,
            'correlation_id': '122',
            }
        self.encoder.write_message_properties(properties)
        self.assertEqual(self.encoder.payload.getvalue(), b'\x1c\x00\x02\x00\x03122')

    def test_write_message_priority_zero(self):
        properties = {
            'delivery_mode': 2,
            'priority': 0,
        }
        self.encoder.write_message_properties(properties)
        self.assertEqual(self.encoder.payload.getvalue(),
                         b'\x18\x00\x02\x00')

    def test_write_message_properties_raises_on_invalid_property_name(self):
        properties = {
            'invalid': 'coucou',
        }
        with self.assertRaises(ValueError):
            self.encoder.write_message_properties(properties)


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
