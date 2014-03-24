"""
    Test frame format.
"""

import unittest

from aioamqp.frame import AmqpEncoder


class EncoderTestCase(unittest.TestCase):
    """Test encoding of python builtin objects to AMQP frames."""
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

    def test_write_dict(self):
        self.encoder.write_value({'foo': 'bar', 'bar': 'baz'})
        self.assertIn(self.encoder.payload.getvalue(),
            # 'F' + total size + key (always a string) + value (with type) + ...
            # The keys are not ordered, so the output is not deterministic (two possible values below)
            (b'F\x00\x00\x00\x18\x03barS\x00\x00\x00\x03baz\x03fooS\x00\x00\x00\x03bar',
             b'F\x00\x00\x00\x18\x03fooS\x00\x00\x00\x03bar\x03barS\x00\x00\x00\x03baz'))
