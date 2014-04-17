import asyncio

from .protocol import AmqpProtocol
from .exceptions import *

from .version import __version__
from .version import __packagename__


@asyncio.coroutine
def connect(host, port, username=None, password=None, ssl=False):
    transport, protocol = yield from asyncio.get_event_loop().create_connection(
        AmqpProtocol, host, port)

    return protocol
