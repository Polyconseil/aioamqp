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
def callback(body, envelope, properties):
    print("consumer {} recved {} ({})".format(envelope.consumer_tag, body, envelope.delivery_tag))

@asyncio.coroutine
def receive_log():
    try:
        transport, protocol = yield from aioamqp.connect('localhost', 5672)
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()
    exchange_name = 'direct_logs'
    # TODO let rabbitmq choose the queue name
    queue_name = 'queue-%s' % random.randint(0, 10000)

    yield from channel.exchange(exchange_name, 'direct')

    yield from asyncio.wait_for(channel.queue(queue_name, durable=False, auto_delete=True), timeout=10)

    severities = sys.argv[1:]
    if not severities:
        print("Usage: %s [info] [warning] [error]" % (sys.argv[0],))
        sys.exit(1)

    for severity in severities:
        yield from asyncio.wait_for(channel.queue_bind(exchange_name='direct_logs',
                                    queue_name=queue_name,
                                    routing_key=severity), timeout=10)

    print(' [*] Waiting for logs. To exit press CTRL+C')

    yield from asyncio.wait_for(channel.basic_consume(queue_name, callback=callback), timeout=10)
    yield from asyncio.Event().wait()

loop = asyncio.get_event_loop()
loop.run_until_complete(receive_log())
