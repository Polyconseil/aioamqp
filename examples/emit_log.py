#!/usr/bin/env python
"""
    RabbitMQ.com pub/sub example

    https://www.rabbitmq.com/tutorials/tutorial-three-python.html

"""

import asyncio
import aioamqp

import sys


async def exchange_routing():
    try:
        transport, protocol = await aioamqp.connect('localhost', 5672)
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return

    channel = await protocol.channel()
    exchange_name = 'logs'
    message = ' '.join(sys.argv[1:]) or "info: Hello World!"

    await channel.exchange_declare(exchange_name=exchange_name, type_name='fanout')

    await channel.basic_publish(message, exchange_name=exchange_name, routing_key='')
    print(" [x] Sent %r" % (message,))

    await protocol.close()
    transport.close()


asyncio.get_event_loop().run_until_complete(exchange_routing())
