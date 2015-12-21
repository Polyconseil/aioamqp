"""
    Hello world `receive.py` example implementation using aioamqp.
    See the documentation for more informations.
"""

import asyncio
import aioamqp


@asyncio.coroutine
def callback(channel, body, envelope, properties):
    print(" [x] Received %r" % body)

@asyncio.coroutine
def receive():
    transport, protocol = yield from aioamqp.connect()
    channel = yield from protocol.channel()

    yield from channel.queue_declare(queue_name='hello')

    yield from channel.basic_consume(callback, queue_name='hello')


event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive())
event_loop.run_forever()
