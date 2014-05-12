import asyncio

from .protocol import AmqpProtocol
from .exceptions import *

from .version import __version__
from .version import __packagename__


@asyncio.coroutine
def connect(host='localhost', port=5672, login='guest', password='guest',
            virtualhost='/', login_method='AMQPLAIN', insist=False, protocol_factory=AmqpProtocol):
    """Convenient method to connect to an AMQP broker

        @host:          the host to connect to
        @port:          broker port
        @login:         login
        @password:      password
        @virtualhost:   AMQP virtualhost to use for this connection
        @login_method:  AMQP auth method
        @insist:        Insist on connecting to a server

        Returns: an AmqpProtocol instance
    """
    _transport, protocol = yield from asyncio.get_event_loop().create_connection(
        protocol_factory, host, port)

    yield from protocol.start_connection(host, port, login, password, virtualhost, login_method, insist)

    return protocol
