#!/usr/bin/env python

import asyncio
import aioamqp


@asyncio.coroutine
def receive():
    try:
        protocol = yield from aioamqp.connect('localhost', 5672)
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()
    queue_name = 'py2.queue'

    yield from asyncio.wait_for(channel.queue(queue_name, durable=False, auto_delete=True), timeout=10)

    yield from asyncio.wait_for(channel.basic_consume(queue_name), timeout=10)

    while True:
        consumer_tag, delivery_tag, message = yield from channel.consume()
        print("consumer {} recved {} ({})".format(consumer_tag, message, delivery_tag))


asyncio.get_event_loop().run_until_complete(receive())
