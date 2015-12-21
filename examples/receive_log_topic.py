#!/usr/bin/env python
"""
    Rabbitmq.com pub/sub example

    https://www.rabbitmq.com/tutorials/tutorial-five-python.html

"""

import asyncio
import aioamqp

import random
import sys


@asyncio.coroutine
def callback(channel, body, envelope, properties):
    print("consumer {} received {} ({})".format(envelope.consumer_tag, body, envelope.delivery_tag))


@asyncio.coroutine
def receive_log():
    try:
        transport, protocol = yield from aioamqp.connect('localhost', 5672)
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()
    exchange_name = 'topic_logs'

    yield from channel.exchange(exchange_name, 'topic')

    result = yield from channel.queue(queue_name='', durable=False, auto_delete=True)
    queue_name = result['queue']

    binding_keys = sys.argv[1:]
    if not binding_keys:
        print("Usage: %s [binding_key]..." % (sys.argv[0],))
        sys.exit(1)

    for binding_key in binding_keys:
        yield from channel.queue_bind(
            exchange_name='topic_logs',
            queue_name=queue_name,
            routing_key=binding_key
        )

    print(' [*] Waiting for logs. To exit press CTRL+C')

    yield from channel.basic_consume(callback, queue_name=queue_name)

event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive_log())
event_loop.run_forever()
