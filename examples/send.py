"""
    Hello world `send.py` example implementation using aioamqp.
    See the documentation for more informations.

"""

import asyncio
import aioamqp


async def send():
    transport, protocol = await aioamqp.connect()
    channel = await protocol.channel()

    await channel.queue_declare(queue_name='hello')

    await channel.basic_publish(
        payload='Hello World!',
        exchange_name='',
        routing_key='hello'
    )

    print(" [x] Sent 'Hello World!'")
    await protocol.close()
    transport.close()



asyncio.get_event_loop().run_until_complete(send())
