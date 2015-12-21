#!/usr/bin/env python
"""
    Rabbitmq.com pub/sub example

    https://www.rabbitmq.com/tutorials/tutorial-four-python.html
"""

import asyncio
import aioamqp

import sys


@asyncio.coroutine
def exchange_routing():
    try:
        transport, protocol = yield from aioamqp.connect('localhost', 5672)
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()
    exchange_name = 'direct_logs'
    severity = sys.argv[1] if len(sys.argv) > 1 else 'info'
    message = ' '.join(sys.argv[2:]) or 'Hello World!'

    yield from channel.exchange(exchange_name, 'direct')

    yield from channel.publish(message, exchange_name=exchange_name, routing_key=severity)
    print(" [x] Sent %r" % (message,))

    yield from protocol.close()
    transport.close()


asyncio.get_event_loop().run_until_complete(exchange_routing())

