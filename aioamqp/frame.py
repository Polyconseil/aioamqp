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

from . import exceptions
from . import constants as amqp_constants


DUMP_FRAMES = False


class AmqpEncoder:

    def __init__(self, writer=None):
        self.payload = io.BytesIO()

    def write_table(self, data_dict):
        if data_dict is None:
            data_dict = {}

        table_encoder = AmqpEncoder()
        for key, value in data_dict.items():
            table_encoder.write_shortstr(key)
            table_encoder.write_value(value)

        table_len = table_encoder.payload.tell()
        self.write_long(table_len)
        if table_len:
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

    def write_bits(self, *args):
        """Write consecutive bools to one byte"""
        assert len(args) <= 8, "write_bits can only write 8 bits into one octet, sadly"
        byte_value = 0

        for arg_index, bit in enumerate(args):
            if bit:
                byte_value |= (1 << arg_index)

        self.write_octet(byte_value)

    def write_bool(self, value):
        self.payload.write(struct.pack('?', value))

    def write_octet(self, octet):
        self.payload.write(struct.pack('!B', octet))

    def write_short(self, short):
        self.payload.write(struct.pack('!H', short))

    def write_long(self, integer):
        self.payload.write(struct.pack('!I', integer))

    def write_long_long(self, longlong):
        self.payload.write(struct.pack('!Q', longlong))

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

    def write_message_properties(self, properties):
        properties_flag_value = 0
        properties_encoder = AmqpEncoder()
        if properties is None:
            self.write_short(0)
            return

        diff = set(properties.keys()) - set(amqp_constants.MESSAGE_PROPERTIES)
        if diff:
            raise ValueError("%s are not properties, valid properties are %s" % (
                diff, amqp_constants.MESSAGE_PROPERTIES))

        content_type = properties.get('content_type')
        if content_type:
            properties_flag_value |= amqp_constants.FLAG_CONTENT_TYPE
            properties_encoder.write_shortstr(content_type)
        content_encoding = properties.get('content_encoding')
        if content_encoding:
            properties_flag_value |= amqp_constants.FLAG_CONTENT_ENCODING
            properties_encoder.write_shortstr(content_encoding)
        headers = properties.get('headers')
        if headers:
            properties_flag_value |= amqp_constants.FLAG_HEADERS
            properties_encoder.write_table(headers)
        delivery_mode = properties.get('delivery_mode')
        if delivery_mode:
            properties_flag_value |= amqp_constants.FLAG_DELIVERY_MODE
            properties_encoder.write_octet(delivery_mode)
        priority = properties.get('priority')
        if priority:
            properties_flag_value |= amqp_constants.FLAG_PRIORITY
            properties_encoder.write_octet(priority)
        correlation_id = properties.get('correlation_id')
        if correlation_id:
            properties_flag_value |= amqp_constants.FLAG_CORRELATION_ID
            properties_encoder.write_octet(correlation_id)
        reply_to = properties.get('reply_to')
        if reply_to:
            properties_flag_value |= amqp_constants.FLAG_REPLY_TO
            properties_encoder.write_shortstr(reply_to)
        expiration = properties.get('expiration')
        if expiration:
            properties_flag_value |= amqp_constants.FLAG_EXPIRATION
            properties_encoder.write_shortstr(expiration)
        message_id = properties.get('message_id')
        if message_id:
            properties_flag_value |= amqp_constants.FLAG_MESSAGE_ID
            properties_encoder.write_shortstr(message_id)
        timestamp = properties.get('timestamp')
        if timestamp:
            properties_flag_value |= amqp_constants.FLAG_TIMESTAMP
            properties_encoder.write_long_long(timestamp)
        type_ = properties.get('type')
        if type_:
            properties_flag_value |= amqp_constants.FLAG_TYPE
            properties_encoder.write_shortstr(type_)
        user_id = properties.get('user_id')
        if user_id:
            properties_flag_value |= amqp_constants.FLAG_USER_ID
            properties_encoder.write_shortstr(user_id)
        app_id = properties.get('app_id')
        if app_id:
            properties_flag_value |= amqp_constants.FLAG_APP_ID
            properties_encoder.write_shortstr(app_id)
        cluster_id = properties.get('cluster_id')
        if cluster_id:
            properties_flag_value |= amqp_constants.FLAG_CLUSTER_ID
            properties_encoder.write_shortstr(cluster_id)

        self.write_short(properties_flag_value)
        self.payload.write(properties_encoder.payload.getvalue())


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

    def read_long_long(self):
        data = self.reader.read(8)
        return struct.unpack('!Q', data)[0]

    def read_float(self):
        data = self.reader.read(4)
        return struct.unpack('!d', data)[0]

    def read_shortstr(self):
        data = self.reader.read(1)
        string_len = struct.unpack('!B', data)[0]
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

        return table

    def read_table_subitem(self, table_data):
        """Read `table_data` bytes, guess the type of the value, and cast it.

            table_data:     a pair of b'<type><value>'
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
        self.weight = None
        self.method_id = None
        self.payload = None
        self.next_body_size = None

    def declare_class(self, class_id, weight=0):
        self.class_id = class_id
        self.weight = 0

    def set_body_size(self, size):
        self.next_body_size = size

    def declare_method(self, class_id, method_id):
        self.class_id = class_id
        self.method_id = method_id

    def write_frame(self, encoder=None):
        payload = None
        if encoder is not None:
            payload = encoder.payload
        content_header = ''
        transmission = io.BytesIO()
        if self.frame_type == amqp_constants.TYPE_METHOD:
            content_header = struct.pack('!HH', self.class_id, self.method_id)
        elif self.frame_type == amqp_constants.TYPE_HEADER:
            content_header = struct.pack('!HHQ', self.class_id, self.weight, self.next_body_size)
        elif self.frame_type == amqp_constants.TYPE_BODY:
            # no specific headers
            pass
        elif self.frame_type == amqp_constants.TYPE_HEARTBEAT:
            # no specific headers
            pass
        else:
            raise Exception("frame_type {} not handled".format(self.frame_type))

        header = struct.pack('!BHI', self.frame_type, self.channel, payload.tell() + len(content_header))
        transmission.write(header)
        if content_header:
            transmission.write(content_header)
        if payload:
            transmission.write(payload.getvalue())
        transmission.write(amqp_constants.FRAME_END)
        return self.writer.write(transmission.getvalue())


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
        self.arguments = {}
        self.frame_length = 0

    @asyncio.coroutine
    def read_frame(self):
        """Decode the frame"""
        try:
            data = yield from self.reader.readexactly(7)
        except (asyncio.IncompleteReadError, socket.error) as ex:
            raise exceptions.AmqpClosedConnection() from ex

        frame_header = io.BytesIO(data)
        decoder = AmqpDecoder(frame_header)
        self.frame_type = decoder.read_octet()
        self.channel = decoder.read_short()
        self.frame_length = decoder.read_long()
        payload_data = yield from self.reader.readexactly(self.frame_length)

        if self.frame_type == amqp_constants.TYPE_METHOD:
            self.payload = io.BytesIO(payload_data)
            decoder = AmqpDecoder(self.payload)
            self.class_id = decoder.read_short()
            self.method_id = decoder.read_short()
            if self.class_id == amqp_constants.CLASS_QUEUE:
                if self.method_id == amqp_constants.QUEUE_DECLARE_OK:
                    self.arguments = {
                        'queue': decoder.read_shortstr(),
                        'message_count': decoder.read_long(),
                        'consumer_count': decoder.read_long(),
                    }
            elif self.class_id == amqp_constants.CLASS_CHANNEL:
                if self.method_id == amqp_constants.CHANNEL_CLOSE:
                    self.arguments = {
                        'reply_code': decoder.read_short(),
                        'reply_text': decoder.read_shortstr(),
                        'class_id': decoder.read_short(),
                        'method_id': decoder.read_short(),
                    }
            elif self.class_id == amqp_constants.CLASS_BASIC:
                if self.method_id == amqp_constants.BASIC_CONSUME_OK:
                    self.arguments = {
                        'consumer_tag': decoder.read_shortstr(),
                    }
                elif self.method_id == amqp_constants.BASIC_CANCEL:
                    self.arguments = {
                        'consumer_tag': decoder.read_shortstr(),
                    }

        elif self.frame_type == amqp_constants.TYPE_HEADER:
            self.payload = io.BytesIO(payload_data)
            decoder = AmqpDecoder(self.payload)
            self.class_id = decoder.read_short()
            self.weight = decoder.read_short()
            self.body_size = decoder.read_long_long()
            self.property_flags = decoder.read_short()

        elif self.frame_type == amqp_constants.TYPE_BODY:
            self.payload = payload_data

        elif self.frame_type == amqp_constants.TYPE_HEARTBEAT:
            pass

        else:
            raise ValueError("Message type {:x} not known".format(self.frame_type))
        self.frame_end = yield from self.reader.readexactly(1)
        assert self.frame_end == amqp_constants.FRAME_END

    def frame(self):
        if not DUMP_FRAMES:
            return
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
