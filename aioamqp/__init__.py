import asyncio

from .protocol import AmqpProtocol
from .exceptions import *

__version__ = '0.0.2'
__packagename__ = 'aioamqp'


@asyncio.coroutine
def connect(host, port, username=None, password=None, ssl=False):
    transport, protocol = yield from asyncio.get_event_loop().create_connection(
        AmqpProtocol, host, port)

    return protocol
