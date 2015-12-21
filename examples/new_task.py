#!/usr/bin/env python

import asyncio
import aioamqp

import sys


@asyncio.coroutine
def new_task():
    try:
        transport, protocol = yield from aioamqp.connect('localhost', 5672)
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return


    channel = yield from protocol.channel()

    yield from channel.queue('task_queue', durable=True)

    message = ' '.join(sys.argv[1:]) or "Hello World!"

    yield from channel.basic_publish(
        payload=message,
        exchange_name='',
        routing_key='task_queue',
        properties={
            'delivery_mode': 2,
        },
    )
    print(" [x] Sent %r" % message,)

    yield from protocol.close()
    transport.close()


asyncio.get_event_loop().run_until_complete(new_task())


