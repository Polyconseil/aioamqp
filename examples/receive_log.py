#!/usr/bin/env python
"""
    Rabbitmq.com pub/sub example

    https://www.rabbitmq.com/tutorials/tutorial-three-python.html

"""

import asyncio
import aioamqp
import random


@asyncio.coroutine
def receive_log():
    protocol = yield from aioamqp.connect('localhost', 5672)

    try:
        yield from protocol.start_connection()
    except aioamqp.ClosedConnection:
        print("closed connection")
        return

    channel = yield from protocol.channel()
    exchange_name = 'logs'
    # TODO let rabbitmq choose the queue name
    queue_name = 'queue-%s' % random.randint(0,10000)

    yield from channel.exchange(exchange_name, 'fanout')

    yield from asyncio.wait_for(channel.queue(queue_name, durable=False, auto_delete=True), timeout=10)

    yield from asyncio.wait_for(channel.queue_bind(exchange_name=exchange_name, queue_name=queue_name, routing_key=''), timeout=10)

    print(' [*] Waiting for logs. To exit press CTRL+C')

    yield from asyncio.wait_for(channel.basic_consume(queue_name), timeout=10)

    while True:
        consumer_tag, delivery_tag, message = yield from channel.consume()
        print("consumer {} recved {} ({})".format(consumer_tag, message, delivery_tag))
    yield from asyncio.sleep(10)
    yield from asyncio.wait_for(protocol.client_close(), timeout=10)

asyncio.get_event_loop().run_until_complete(receive_log())
