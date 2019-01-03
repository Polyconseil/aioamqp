#!/usr/bin/env python
"""
    Rabbitmq.com pub/sub example

    https://www.rabbitmq.com/tutorials/tutorial-three-python.html

"""

import asyncio
import aioamqp
import random



async def callback(channel, body, envelope, properties):
    print(" [x] %r" % body)


async def receive_log():
    try:
        transport, protocol = await aioamqp.connect('localhost', 5672)
    except aioamqp.AmqpClosedConnection:
        print("closed connections")
        return


    channel = await protocol.channel()
    exchange_name = 'logs'

    await channel.exchange(exchange_name=exchange_name, type_name='fanout')

    # let RabbitMQ generate a random queue name
    result = await channel.queue(queue_name='', exclusive=True)

    queue_name = result['queue']
    await channel.queue_bind(exchange_name=exchange_name, queue_name=queue_name, routing_key='')

    print(' [*] Waiting for logs. To exit press CTRL+C')

    await channel.basic_consume(callback, queue_name=queue_name, no_ack=True)

event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive_log())
event_loop.run_forever()
