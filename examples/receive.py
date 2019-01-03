"""
    Hello world `receive.py` example implementation using aioamqp.
    See the documentation for more informations.
"""

import asyncio
import aioamqp


async def callback(channel, body, envelope, properties):
    print(" [x] Received %r" % body)

async def receive():
    transport, protocol = await aioamqp.connect()
    channel = await protocol.channel()

    await channel.queue_declare(queue_name='hello')

    await channel.basic_consume(callback, queue_name='hello')


event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive())
event_loop.run_forever()
