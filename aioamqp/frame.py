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

import pamqp.encode
import pamqp.frame

from . import exceptions
from . import constants as amqp_constants


DUMP_FRAMES = False


def write(writer, channel, encoder):
    """Writes the built frame from the encoder

        writer:     asyncio StreamWriter
        channel:    amqp Channel identifier
        encoder:    frame encoder from pamqp which can be marshalled

    Returns int, the number of bytes written.
    """
    return writer.write(pamqp.frame.marshal(encoder, channel))


async def read(reader):
    """Read a new frame from the wire

        reader:     asyncio StreamReader

    Returns (channel, frame) a tuple containing both channel and the pamqp frame,
                             the object describing the frame
    """
    if not reader:
        raise exceptions.AmqpClosedConnection()
    try:
        data = await reader.readexactly(7)
    except (asyncio.IncompleteReadError, OSError) as ex:
        raise exceptions.AmqpClosedConnection() from ex

    frame_type, channel, frame_length = pamqp.frame.frame_parts(data)

    payload_data = await reader.readexactly(frame_length)
    frame = None

    if frame_type == amqp_constants.TYPE_METHOD:
        frame = pamqp.frame._unmarshal_method_frame(payload_data)

    elif frame_type == amqp_constants.TYPE_HEADER:
        frame = pamqp.frame._unmarshal_header_frame(payload_data)

    elif frame_type == amqp_constants.TYPE_BODY:
        frame = pamqp.frame._unmarshal_body_frame(payload_data)

    elif frame_type == amqp_constants.TYPE_HEARTBEAT:
        frame = pamqp.heartbeat.Heartbeat()

    frame_end = await reader.readexactly(1)
    assert frame_end == amqp_constants.FRAME_END
    return channel, frame
