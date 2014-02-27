#!/usr/bin/env python

import asyncio
import aioamqp

import sys


@asyncio.coroutine
def exchange_routing():
    protocol = yield from aioamqp.connect('localhost', 5672)

    try:
        yield from protocol.start_connection()
    except aioamqp.ClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()
    queue_name = "task_queue"
    message = ' '.join(sys.argv[1:]) or "Hello World!"
    message_properties = {
        'delivery_mode': 2
    }

    yield from asyncio.wait_for(channel.queue(queue_name, durable=True), timeout=10)
    yield from asyncio.sleep(2)

    yield from channel.publish("Message ", '', queue_name, properties=message_properties)
    print(" [x] Sent %r" % (message,))

    yield from asyncio.sleep(1)
    yield from asyncio.wait_for(protocol.client_close(), timeout=10)


asyncio.get_event_loop().run_until_complete(exchange_routing())
