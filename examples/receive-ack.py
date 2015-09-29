#!/usr/bin/env python

import asyncio
import aioamqp

@asyncio.coroutine
def callback(channel, body, envelope, properties):
    yield from channel.basic_client_ack(envelope.delivery_tag)

@asyncio.coroutine
def receive():
    try:
        transport, protocol = yield from aioamqp.connect('localhost', 5672)
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()
    queue_name = 'py2.queue'

    yield from asyncio.wait_for(
        channel.queue(queue_name, durable=True, auto_delete=False), timeout=10
    )

    yield from channel.basic_consume(queue_name, callback=callback)

asyncio.get_event_loop().run_until_complete(receive())
asyncio.get_event_loop().run_forever()
