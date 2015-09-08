import asyncio
import sys
import ssl as ssl_module  # import as to enable argument named ssl in connect
from urllib.parse import urlparse

from .exceptions import *
from .protocol import AmqpProtocol

from .version import __version__
from .version import __packagename__


@asyncio.coroutine
def connect(host='localhost', port=None, login='guest', password='guest',
            virtualhost='/', ssl=False, login_method='AMQPLAIN', insist=False,
            protocol_factory=AmqpProtocol, *, verify_ssl=True, **kwargs):
    """Convenient method to connect to an AMQP broker

        @host:          the host to connect to
        @port:          broker port
        @login:         login
        @password:      password
        @virtualhost:   AMQP virtualhost to use for this connection
        @ssl:           Create an SSL connection instead of a plain unencrypted one
        @verify_ssl:    Verify server's SSL certificate (True by default)
        @login_method:  AMQP auth method
        @insist:        Insist on connecting to a server

        @kwargs:        Arguments to be given to the protocol_factory instance

        Returns:        a tuple (transport, protocol) of an AmqpProtocol instance
    """
    if kwargs:
        factory = lambda: protocol_factory(**kwargs)
    else:
        factory = protocol_factory

    create_connection_kwargs = {}

    if ssl:
        if sys.version_info < (3, 4):
            raise NotImplementedError('SSL not supported on Python 3.3 yet')
        ssl_context = ssl_module.create_default_context()
        if not verify_ssl:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl_module.CERT_NONE
        create_connection_kwargs['ssl'] = ssl_context

    if port is None:
        if ssl:
            port = 5671
        else:
            port = 5672

    transport, protocol = yield from asyncio.get_event_loop().create_connection(
        factory, host, port, **create_connection_kwargs
    )

    yield from protocol.start_connection(host, port, login, password, virtualhost, ssl=ssl,
        login_method=login_method, insist=insist)

    return (transport, protocol)


@asyncio.coroutine
def from_url(
        url, login_method='AMQPLAIN', insist=False, protocol_factory=AmqpProtocol, *,
        verify_ssl=True):
    """ Connect to the AMQP using a single url parameter and return the client.

        For instance:

            amqp://user:password@hostname:port/vhost

        Returns:        a tuple (transport, protocol) of an AmqpProtocol instance
    """
    url = urlparse(url)

    if url.scheme not in ('amqp', 'amqps'):
        raise ValueError('Invalid protocol %s, valid protocols are amqp or amqps' % url.scheme)

    transport, protocol = yield from connect(
        host=url.hostname or 'localhost',
        port=url.port,
        login=url.username or 'guest',
        password=url.password or 'guest',
        virtualhost=(url.path[1:] if len(url.path) > 1 else '/'),
        ssl=(url.scheme == 'amqps'),
        login_method=login_method,
        insist=insist,
        protocol_factory=protocol_factory,
        verify_ssl=verify_ssl)
    return transport, protocol
