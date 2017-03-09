"""
    Hello world `receive.py` example implementation using aioamqp.
    See the documentation for more informations.
"""

import asyncio
import aioamqp


@asyncio.coroutine
def receive():
    transport, protocol = yield from aioamqp.connect()
    channel = yield from protocol.channel()

    yield from channel.queue_declare(queue_name='hello')

    consumer = yield from channel.basic_consume(queue_name='hello')

    while (yield from consumer.fetch_message()):
        channel, body, envelope, properties = consumer.get_message()
        print(" [x] Received %r" % body)

event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive())
event_loop.run_forever()
