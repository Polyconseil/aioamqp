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
        protocol = yield from aioamqp.connect('localhost', 5672)
    except aioamqp.ClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()
    exchange_name = 'direct_logs'
    severity = sys.argv[1] if len(sys.argv) > 1 else 'info'
    message = ' '.join(sys.argv[2:]) or 'Hello World!'

    yield from channel.exchange(exchange_name, 'direct')
    yield from asyncio.sleep(2)

    yield from channel.publish(
        message, exchange_name=exchange_name, routing_key=severity)
    print(" [x] Sent %r" % (message,))

    yield from asyncio.sleep(1)
    yield from asyncio.wait_for(protocol.client_close(), timeout=10)


asyncio.get_event_loop().run_until_complete(exchange_routing())

