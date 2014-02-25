#!/usr/bin/env python

"""
    Amqp client exemple

"""
import asyncio
import aioamqp


@asyncio.coroutine
def amqp_test():
    protocol = yield from aioamqp.connect('localhost', 5672)

    try:
        yield from protocol.start_connection()
    except aioamqp.ClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()

    yield from asyncio.wait_for(channel.exchange("aioamqp.exchange", "fanout"), timeout=10)
    yield from asyncio.wait_for(channel.queue("queue"), timeout=10)

    yield from asyncio.sleep(2)
    yield from asyncio.wait_for(channel.queue_bind("queue", "aioamqp.exchange", "routing_key"), timeout=10)

    yield from channel.publish("Message", 'aioamqp.exchange', 'routing_key')

    yield from asyncio.sleep(14)
    yield from asyncio.wait_for(protocol.client_close(), timeout=10)


asyncio.get_event_loop().run_until_complete(amqp_test())
