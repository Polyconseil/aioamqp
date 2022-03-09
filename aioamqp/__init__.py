import asyncio
import socket
import ssl
from urllib.parse import urlparse

from .exceptions import *  # pylint: disable=wildcard-import
from .protocol import AmqpProtocol

from .version import __version__
from .version import __packagename__


async def connect(host='localhost', port=None, login='guest', password='guest',
            virtualhost='/', ssl=None, login_method='PLAIN', insist=False,  # pylint: disable=redefined-outer-name
            protocol_factory=AmqpProtocol, **kwargs):
    """Convenient method to connect to an AMQP broker

        @host:          the host to connect to
        @port:          broker port
        @login:         login
        @password:      password
        @virtualhost:   AMQP virtualhost to use for this connection
        @ssl:           SSL context used for secure connections, omit for no SSL
                        - see https://docs.python.org/3/library/ssl.html
        @login_method:  AMQP auth method
        @insist:        Insist on connecting to a server
        @protocol_factory:
                        Factory to use, if you need to subclass AmqpProtocol

        @kwargs:        Arguments to be given to the protocol_factory instance

        Returns:        a tuple (transport, protocol) of an AmqpProtocol instance
    """
    factory = lambda: protocol_factory(**kwargs)  # pylint: disable=unnecessary-lambda

    create_connection_kwargs = {}

    if ssl is not None:
        create_connection_kwargs['ssl'] = ssl

    if port is None:
        if ssl:
            port = 5671
        else:
            port = 5672

    transport, protocol = await asyncio.get_running_loop().create_connection(
        factory, host, port, **create_connection_kwargs
    )

    # these 2 flags *may* show up in sock.type. They are only available on linux
    # see https://bugs.python.org/issue21327
    nonblock = getattr(socket, 'SOCK_NONBLOCK', 0)
    cloexec = getattr(socket, 'SOCK_CLOEXEC', 0)
    sock = transport.get_extra_info('socket')
    if sock is not None and (sock.type & ~nonblock & ~cloexec) == socket.SOCK_STREAM:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    try:
        await protocol.start_connection(host, port, login, password, virtualhost, ssl=ssl, login_method=login_method,
                                        insist=insist)
    except Exception:
        await protocol.wait_closed()
        raise

    return transport, protocol


async def from_url(
        url, login_method='PLAIN', insist=False, protocol_factory=AmqpProtocol, **kwargs):
    """ Connect to the AMQP using a single url parameter and return the client.

        For instance:

            amqp://user:password@hostname:port/vhost

        @insist:        Insist on connecting to a server
        @protocol_factory:
                        Factory to use, if you need to subclass AmqpProtocol

        @kwargs:        Arguments to be given to the protocol_factory instance

        Returns:        a tuple (transport, protocol) of an AmqpProtocol instance
    """
    url = urlparse(url)

    if url.scheme not in ('amqp', 'amqps'):
        raise ValueError(f'Invalid protocol {url.scheme}, valid protocols are amqp or amqps')

    if url.scheme == 'amqps' and not kwargs.get('ssl'):
        kwargs['ssl'] = ssl.create_default_context()

    transport, protocol = await connect(
        host=url.hostname or 'localhost',
        port=url.port,
        login=url.username or 'guest',
        password=url.password or 'guest',
        virtualhost=(url.path[1:] if len(url.path) > 1 else '/'),
        login_method=login_method,
        insist=insist,
        protocol_factory=protocol_factory,
        **kwargs)
    return transport, protocol
