"""
    Helper class to decode AMQP responses

AMQP Frame implementations


0      1         3         7                      size+7      size+8
+------+---------+---------+    +-------------+     +-----------+
| type | channel |   size  |    |   payload   |     | frame-end |
+------+---------+---------+    +-------------+     +-----------+
 octets   short     long         'size' octets          octet

The frame-end octet MUST always be the hexadecimal value %xCE

type:

Type = 1, "METHOD": method frame.
Type = 2, "HEADER": content header frame.
Type = 3, "BODY": content body frame.
Type = 4, "HEARTBEAT": heartbeat frame.


Method Payload

0          2           4
+----------+-----------+-------------- - -
| class-id | method-id | arguments...
+----------+-----------+-------------- - -
    short     short       ...

Content Payload

0          2        4           12               14
+----------+--------+-----------+----------------+------------- - -
| class-id | weight | body size | property flags | property list...
+----------+--------+-----------+----------------+------------- - -
   short     short    long long       short        remainder...

"""

import asyncio
import io
import struct
import socket
import os
import datetime
from itertools import count
from decimal import Decimal

import pamqp.encode
import pamqp.specification
import pamqp.frame

from . import exceptions
from . import constants as amqp_constants
from .properties import Properties


DUMP_FRAMES = False


class AmqpDecoder:
    def __init__(self, reader):
        self.reader = reader

    def read_bit(self):
        return bool(self.read_octet())

    def read_octet(self):
        data = self.reader.read(1)
        return ord(data)

    def read_signed_octet(self):
        data = self.reader.read(1)
        return struct.unpack('!b', data)[0]

    def read_short(self):
        data = self.reader.read(2)
        return struct.unpack('!H', data)[0]

    def read_signed_short(self):
        data = self.reader.read(2)
        return struct.unpack('!h', data)[0]

    def read_long(self):
        data = self.reader.read(4)
        return struct.unpack('!I', data)[0]

    def read_signed_long(self):
        data = self.reader.read(4)
        return struct.unpack('!i', data)[0]

    def read_long_long(self):
        data = self.reader.read(8)
        return struct.unpack('!Q', data)[0]

    def read_signed_long_long(self):
        data = self.reader.read(8)
        return struct.unpack('!q', data)[0]

    def read_float(self):
        # XXX: This used to read & unpack '!d', which is a double, not a shorter float
        data = self.reader.read(4)
        return struct.unpack('!f', data)[0]

    def read_double(self):
        data = self.reader.read(8)
        return struct.unpack('!d', data)[0]

    def read_decimal(self):
        decimals = self.read_octet()
        value = self.read_signed_long()
        return Decimal(value) * (Decimal(10) ** -decimals)

    def read_shortstr(self):
        data = self.reader.read(1)
        string_len = struct.unpack('!B', data)[0]
        data = self.reader.read(string_len)
        return data.decode()

    def read_longstr(self):
        string_len = self.read_long()
        data = self.reader.read(string_len)
        return data.decode()

    def read_timestamp(self):
        return datetime.datetime.fromtimestamp(self.read_long_long(), datetime.timezone.utc)

    def read_table(self):
        """Reads an AMQP table"""
        table_len = self.read_long()
        table_data = AmqpDecoder(io.BytesIO(self.reader.read(table_len)))
        table = {}
        while table_data.reader.tell() < table_len:
            var_name = table_data.read_shortstr()
            var_value = self.read_table_subitem(table_data)
            table[var_name] = var_value
        return table

    _table_subitem_reader_map = {
        't': 'read_bit',
        'b': 'read_octet',
        'B': 'read_signed_octet',
        'U': 'read_signed_short',
        'u': 'read_short',
        'I': 'read_signed_long',
        'i': 'read_long',
        'L': 'read_unsigned_long_long',
        'l': 'read_long_long',
        'f': 'read_float',
        'd': 'read_float',
        'D': 'read_decimal',
        's': 'read_shortstr',
        'S': 'read_longstr',
        'A': 'read_field_array',
        'T': 'read_timestamp',
        'F': 'read_table',
    }

    def read_table_subitem(self, table_data):
        """Read `table_data` bytes, guess the type of the value, and cast it.

            table_data:     a pair of b'<type><value>'
        """
        value_type = chr(table_data.read_octet())
        if value_type == 'V':
            return None
        else:
            reader_name = self._table_subitem_reader_map.get(value_type)
            if not reader_name:
                raise ValueError('Unknown value_type {}'.format(value_type))
            return getattr(table_data, reader_name)()

    def read_field_array(self):
        array_len = self.read_long()
        array_data = AmqpDecoder(io.BytesIO(self.reader.read(array_len)))
        field_array = []
        while array_data.reader.tell() < array_len:
            item = self.read_table_subitem(array_data)
            field_array.append(item)
        return field_array


def write(writer, channel, encoder):
    """Writes the built frame from the encoder

        writer:     asyncio StreamWriter
        channel:    amqp Channel identifier
        encoder:    frame encoder from pamqp which can be marshalled

    Returns int, the number of bytes written.
    """
    return writer.write(pamqp.frame.marshal(encoder, channel))


class AmqpResponse:
    """Read a response from the AMQP server

    """
    def __init__(self, reader):
        self.reader = reader
        self.frame_type = None
        self.channel = 0  # default channel in AMQP
        self.payload_size = None
        self.frame_end = None
        self.frame_payload = None
        self.payload = None
        self.frame_class = None
        self.frame_method = None
        self.class_id = None
        self.method_id = None
        self.weight = None
        self.body_size = None
        self.property_flags = None
        self.properties = None
        self.arguments = {}
        self.frame_length = 0

        self.payload_decoder = None
        self.header_decoder = None

    @asyncio.coroutine
    def read_frame(self):
        """Decode the frame"""
        if not self.reader:
            raise exceptions.AmqpClosedConnection()
        try:
            data = yield from self.reader.readexactly(7)
        except (asyncio.IncompleteReadError, socket.error) as ex:
            raise exceptions.AmqpClosedConnection() from ex

        frame_header = io.BytesIO(data)
        self.header_decoder = AmqpDecoder(frame_header)
        self.frame_type = self.header_decoder.read_octet()
        self.channel = self.header_decoder.read_short()
        self.frame_length = self.header_decoder.read_long()
        payload_data = yield from self.reader.readexactly(self.frame_length)

        if self.frame_type == amqp_constants.TYPE_METHOD:
            self.payload = io.BytesIO(payload_data)
            self.payload_decoder = AmqpDecoder(self.payload)
            self.class_id = self.payload_decoder.read_short()
            self.method_id = self.payload_decoder.read_short()

        elif self.frame_type == amqp_constants.TYPE_HEADER:
            self.payload = io.BytesIO(payload_data)
            self.payload_decoder = AmqpDecoder(self.payload)
            self.class_id = self.payload_decoder.read_short()
            self.weight = self.payload_decoder.read_short()
            self.body_size = self.payload_decoder.read_long_long()
            self.property_flags = 0
            for flagword_index in count(0):
                partial_flags = self.payload_decoder.read_short()
                self.property_flags |= partial_flags << (flagword_index * 16)
                if partial_flags & 1 == 0:
                    break
            decoded_properties = {}
            if self.property_flags & amqp_constants.FLAG_CONTENT_TYPE:
                decoded_properties['content_type'] = self.payload_decoder.read_shortstr()
            if self.property_flags & amqp_constants.FLAG_CONTENT_ENCODING:
                decoded_properties['content_encoding'] = self.payload_decoder.read_shortstr()
            if self.property_flags & amqp_constants.FLAG_HEADERS:
                decoded_properties['headers'] = self.payload_decoder.read_table()
            if self.property_flags & amqp_constants.FLAG_DELIVERY_MODE:
                decoded_properties['delivery_mode'] = self.payload_decoder.read_octet()
            if self.property_flags & amqp_constants.FLAG_PRIORITY:
                decoded_properties['priority'] = self.payload_decoder.read_octet()
            if self.property_flags & amqp_constants.FLAG_CORRELATION_ID:
                decoded_properties['correlation_id'] = self.payload_decoder.read_shortstr()
            if self.property_flags & amqp_constants.FLAG_REPLY_TO:
                decoded_properties['reply_to'] = self.payload_decoder.read_shortstr()
            if self.property_flags & amqp_constants.FLAG_EXPIRATION:
                decoded_properties['expiration'] = self.payload_decoder.read_shortstr()
            if self.property_flags & amqp_constants.FLAG_MESSAGE_ID:
                decoded_properties['message_id'] = self.payload_decoder.read_shortstr()
            if self.property_flags & amqp_constants.FLAG_TIMESTAMP:
                decoded_properties['timestamp'] = self.payload_decoder.read_long_long()
            if self.property_flags & amqp_constants.FLAG_TYPE:
                decoded_properties['type'] = self.payload_decoder.read_shortstr()
            if self.property_flags & amqp_constants.FLAG_USER_ID:
                decoded_properties['user_id'] = self.payload_decoder.read_shortstr()
            if self.property_flags & amqp_constants.FLAG_APP_ID:
                decoded_properties['app_id'] = self.payload_decoder.read_shortstr()
            if self.property_flags & amqp_constants.FLAG_CLUSTER_ID:
                decoded_properties['cluster_id'] = self.payload_decoder.read_shortstr()
            self.properties = Properties(**decoded_properties)

        elif self.frame_type == amqp_constants.TYPE_BODY:
            self.payload = payload_data

        elif self.frame_type == amqp_constants.TYPE_HEARTBEAT:
            pass

        else:
            raise ValueError("Message type {:x} not known".format(self.frame_type))
        self.frame_end = yield from self.reader.readexactly(1)
        assert self.frame_end == amqp_constants.FRAME_END

    def __str__(self):
        frame_data = {
            'type': self.frame_type,
            'channel': self.channel,
            'size': self.payload_size,
            'frame_end': self.frame_end,
            'payload': self.frame_payload,
        }
        output = """
0        1           3            7                        size+7        size+8
+--------+-----------+------------+    +---------------+     +--------------+
|{type!r:^8}|{channel!r:^11}|{size!r:^12}|    |{payload!r:^15}|     |{frame_end!r:^14}|
+--------+-----------+------------+    +---------------+     +--------------+
   type    channel       size                payload            frame-end
""".format(**frame_data)

        if self.frame_type == amqp_constants.TYPE_METHOD:
            method_data = {
                'class_id': self.class_id,
                'method_id': self.method_id,
            }
            type_output = """
0          2           4
+----------+-----------+-------------- - -
|{class_id:^10}|{method_id:^11}| arguments...
+----------+-----------+-------------- - -
  class-id   method-id       ...""".format(**method_data)

            output += os.linesep + type_output

        return output
