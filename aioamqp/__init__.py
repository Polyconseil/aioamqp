import asyncio
import ssl as ssl_module
from urllib.parse import urlparse

from .protocol import AmqpProtocol

from .version import __version__
from .version import __packagename__


@asyncio.coroutine
def connect(host='localhost', port=5672, login='guest', password='guest',
            virtualhost='/', ssl=False, login_method='AMQPLAIN', insist=False,
            protocol_factory=AmqpProtocol, **kwargs):
    """Convenient method to connect to an AMQP broker

        @host:          the host to connect to
        @port:          broker port
        @login:         login
        @password:      password
        @virtualhost:   AMQP virtualhost to use for this connection
        @login_method:  AMQP auth method
        @insist:        Insist on connecting to a server

        @kwargs:        Arguments to be given to the protocol_factory instance

        Returns:        a tuple (transport, protocol) of an AmqpProtocol instance
    """
    if ssl:
        ssl_context = ssl_module.create_default_context()
    else:
        ssl_context = None
    if kwargs:
        transport, protocol = yield from asyncio.get_event_loop().create_connection(
        lambda: protocol_factory(**kwargs), host, port, ssl=ssl_context)

    else:
        transport, protocol = yield from asyncio.get_event_loop().create_connection(
        protocol_factory, host, port, ssl=ssl_context)

    yield from protocol.start_connection(host, port, login, password, virtualhost, ssl=ssl,
        login_method=login_method, insist=insist)

    return (transport, protocol)


@asyncio.coroutine
def from_url(url, login_method='AMQPLAIN', insist=False, protocol_factory=AmqpProtocol):
    """ Connect to the AMQP using a single url parameter and return the client.
    """
    url = urlparse(url)

    if url.scheme not in ('amqp', 'amqps'):
        raise ValueError('Invalid protocol %s, valid protocols are amqp or amqps' % url.scheme)

    transport, protocol = yield from connect(
        host=url.hostname or 'localhost',
        port=url.port or 5672,
        login=url.username or 'guest',
        password=url.password or 'guest',
        virtualhost=(url.path[1:] if len(url.path) > 1 else '/'),
        ssl=(url.scheme == 'amqps'),
        login_method=login_method,
        insist=insist,
        protocol_factory=protocol_factory)
    return protocol
