#!/usr/bin/env python
"""
    Worker example from the 2nd tutorial
"""

import asyncio
import aioamqp

import sys


@asyncio.coroutine
def worker():
    try:
        transport, protocol = yield from aioamqp.connect('localhost', 5672)
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()

    yield from channel.queue(queue_name='task_queue', durable=True)
    yield from channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)
    consumer = yield from channel.basic_consume(queue_name='task_queue')

    while (yield from consumer.fetch_message()):
        channel, body, envelope, properties = consumer.get_message()
        print(" [x] Received %r" % body)
        yield from asyncio.sleep(body.count(b'.'))
        print(" [x] Done")
        yield from channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(worker())
event_loop.run_forever()
