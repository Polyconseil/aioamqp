#!/usr/bin/env python
"""
    Rabbitmq.com pub/sub example

    https://www.rabbitmq.com/tutorials/tutorial-four-python.html

"""

import asyncio
import aioamqp

import random
import sys


@asyncio.coroutine
def callback(channel, body, envelope, properties):
    print("consumer {} recved {} ({})".format(envelope.consumer_tag, body, envelope.delivery_tag))

@asyncio.coroutine
def receive_log(waiter):
    try:
        transport, protocol = yield from aioamqp.connect('localhost', 5672)
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()
    exchange_name = 'direct_logs'

    yield from channel.exchange(exchange_name, 'direct')

    result = yield from channel.queue(queue_name='', durable=False, auto_delete=True)

    queue_name = result['queue']

    severities = sys.argv[1:]
    if not severities:
        print("Usage: %s [info] [warning] [error]" % (sys.argv[0],))
        sys.exit(1)

    for severity in severities:
        yield from channel.queue_bind(
            exchange_name='direct_logs',
            queue_name=queue_name,
            routing_key=severity,
        )

    print(' [*] Waiting for logs. To exit press CTRL+C')

    yield from asyncio.wait_for(channel.basic_consume(callback, queue_name=queue_name), timeout=10)
    yield from waiter.wait()

    yield from protocol.close()
    transport.close()

loop = asyncio.get_event_loop()

try:
    waiter = asyncio.Event()
    loop.run_until_complete(receive_log(waiter))
except KeyboardInterrupt:
    waiter.set()
