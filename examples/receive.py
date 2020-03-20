"""
    Hello world `receive.py` example implementation using aioamqp.
    See the documentation for more informations.
"""

import asyncio
import aioamqp


async def receive():
    transport, protocol = await aioamqp.connect()
    channel = await protocol.channel()

    await channel.queue_declare(queue_name='hello')

    x = channel.consume(queue_name='hello')
    await x.qos(prefetch_size=0, prefetch_count=1)
    async with x as consumer:
        async for message in consumer:
            body, envelope, properties = message
            await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive())
event_loop.run_forever()
