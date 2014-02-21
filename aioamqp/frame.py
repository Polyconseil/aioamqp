"""
    Helper class to decode AMQP responses

AMQP Frame implementations


0      1         3         7                      size+7      size+8
+------+---------+---------+    +-------------+     +-----------+
| type | channel |   size  |    |   payload   |     | frame-end |
+------+---------+---------+    +-------------+     +-----------+
 octets   short     long         'size' octets          octet

type:

Type = 1, "METHOD": method frame.
Type = 2, "HEADER": content header frame.
Type = 3, "BODY": content body frame.
Type = 4, "HEARTBEAT": heartbeat frame.


Method Payloads

0          2           4
+----------+-----------+-------------- - -
| class-id | method-id | arguments...
+----------+-----------+-------------- - -
    short     short       ...

The frame-end octet MUST always be the hexadecimal value %xCE


"""

import asyncio
import io
import struct

from . import constants as amqp_constants


class AmqpEncoder:

    def __init__(self, writer=None):
        self.payload = io.BytesIO()

    def write_table(self, data_dict):
        table_encoder = AmqpEncoder()
        for key, value in data_dict.items():
            table_encoder.write_shortstr(key)
            table_encoder.write_value(value)

        table_len = table_encoder.payload.tell()
        self.write_long(table_len)
        self.payload.write(table_encoder.payload.getvalue())

    def write_value(self, value):
        if isinstance(value, (bytes, str)):
            self.payload.write(b'S')
            self.write_longstr(value)
        elif isinstance(value, bool):
            self.payload.write(b't')
            self.write_bool(value)
        elif isinstance(value, dict):
            self.payload.write(b'F')
            self.write_table(value)
        else:
            raise Exception("type({}) unsupported".format(type(value)))

    def write_bool(self, value):
        self.payload.write(struct.pack('?', value))

    def write_octet(self, octet):
        self.payload.write(struct.pack('B', octet))

    def write_short(self, short):
        self.payload.write(struct.pack('!H', short))

    def write_long(self, integer):
        self.payload.write(struct.pack('!I', integer))

    def _write_string(self, string):
        if isinstance(string, str):
            self.payload.write(string.encode())
        elif isinstance(string, bytes):
            self.payload.write(string)

    def write_longstr(self, string):
        self.write_long(len(string))
        self._write_string(string)

    def write_shortstr(self, string):
        self.write_octet(len(string))
        self._write_string(string)


class AmqpDecoder:
    def __init__(self, reader):
        self.reader = reader

    def read_octet(self):
        data = self.reader.read(1)
        return ord(data)

    def read_short(self):
        data = self.reader.read(2)
        return struct.unpack('!H', data)[0]

    def read_long(self):
        data = self.reader.read(4)
        return struct.unpack('!I', data)[0]

    def read_float(self):
        data = self.reader.read(4)
        return struct.unpack('!d', data)[0]

    def read_shortstr(self):
        data = self.reader.read(1)
        string_len = struct.unpack('B', data)[0]
        data = self.reader.read(string_len)
        return data.decode()

    def read_longstr(self):
        string_len = self.read_long()
        data = self.reader.read(string_len)
        return data.decode()

    def read_table(self):
        """Reads an AMQP table"""
        table_len = self.read_long()
        table_data = AmqpDecoder(io.BytesIO(self.reader.read(table_len)))
        table = {}
        while table_data.reader.tell() < table_len:
            var_name = table_data.read_shortstr()
            var_value = self.read_table_subitem(table_data)
            table[var_name] = var_value

    def read_table_subitem(self, table_data):
        """

        """
        value_type = chr(table_data.read_octet())
        if value_type == 'F':
            return table_data.read_table()
        elif value_type == 'S':
            return table_data.read_longstr()
        elif value_type == 't':
            return bool(table_data.read_octet())
            #return True
        print("value_type {} unknown".format(value_type))


class AmqpRequest:
    def __init__(self, writer, frame_type, channel):
        self.writer = writer
        self.frame_type = frame_type
        self.channel = channel
        self.class_id = None
        self.method_id = None
        self.payload = None

    def declare_method(self, class_id, method_id):
        self.class_id = class_id
        self.method_id = method_id

    def write_frame(self, encoder):
        if self.frame_type == amqp_constants.TYPE_METHOD:
            transmission = io.BytesIO()
            payload = encoder.payload
            print("size: ", payload.tell())
            header = struct.pack('!BHI', self.frame_type, self.channel, payload.tell() + 4)
            transmission.write(header)
            method_payload = struct.pack('!HH', self.class_id, self.method_id)
            transmission.write(method_payload)
            transmission.write(payload.getvalue())
            transmission.write(struct.pack('>B', amqp_constants.FRAME_END))
            self.writer.write(transmission.getvalue())
        else:
            raise Exception("frame_type {} not handlded".format(self.frame_type))


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

    @asyncio.coroutine
    def read_frame(self):
        """Decode the frame"""
        data = yield from self.reader.readexactly(7)
        frame_header = io.BytesIO(data)
        decoder = AmqpDecoder(frame_header)
        self.frame_type = decoder.read_octet()
        self.channel = decoder.read_short()
        self.payload_size = decoder.read_long()

        if self.frame_type == amqp_constants.TYPE_METHOD:
            payload_data = yield from self.reader.readexactly(self.payload_size)
            self.payload = io.BytesIO(payload_data)
            decoder = AmqpDecoder(self.payload)
            self.class_id = decoder.read_short()
            self.method_id = decoder.read_short()

        self.frame_end = yield from self.reader.readexactly(1)

    def frame(self):
        frame_data = {
            'type': self.frame_type or '',
            'channel': self.channel,
            'size': self.payload_size or '',
            'frame_end': self.frame_end or '',
            'payload': self.frame_payload or '',
        }
        print("""
0        1           3            7                        size+7        size+8
+--------+-----------+------------+    +---------------+     +--------------+
|{type:^8}|{channel:^11}|{size:^12}|    |{payload:^15}|     |{frame_end:^14}|
+--------+-----------+------------+    +---------------+     +--------------+
   type    channel       size                payload            frame-end
""".format(**frame_data))

        if self.frame_type == amqp_constants.TYPE_METHOD:
            method_data = {
                'class_id': self.class_id,
                'method_id': self.method_id,
            }
            print("""
0          2           4
+----------+-----------+-------------- - -
|{class_id:^10}|{method_id:^11}| arguments...
+----------+-----------+-------------- - -
  class-id   method-id       ...""".format(**method_data))
