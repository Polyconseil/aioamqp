#!/usr/bin/env python

"""
    Amqp client exemple

"""
import asyncio
import aioamqp


@asyncio.coroutine
def simple_message_example():
    protocol = yield from aioamqp.connect('localhost', 5672)

    try:
        yield from protocol.start_connection()
    except aioamqp.ClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()
    yield from asyncio.wait_for(channel.queue("py2.queue"), timeout=10)
    frame = yield from protocol.get_frame()
    frame.frame()
    while True:
        yield from channel.publish("py3.message", '', 'py2.queue')
    yield from asyncio.wait_for(protocol.client_close(), timeout=10)


@asyncio.coroutine
def exchange_routing():
    protocol = yield from aioamqp.connect('localhost', 5672)

    try:
        yield from protocol.start_connection()
    except aioamqp.ClosedConnection:
        print("closed connections")
        return

    channel = yield from protocol.channel()

    yield from asyncio.wait_for(channel.exchange("aioamqp.exchange", "fanout"), timeout=10)
    yield from asyncio.wait_for(channel.queue("queue2"), timeout=10)

    yield from asyncio.sleep(2)
    yield from asyncio.wait_for(channel.queue_bind("queue2", "aioamqp.exchange", "routing_key"), timeout=10)

    yield from asyncio.wait_for(channel.publish("Message", 'aioamqp.exchange', 'routing_key'), timeout=10)

    yield from asyncio.sleep(14000)
    yield from asyncio.wait_for(protocol.client_close(), timeout=10)


asyncio.get_event_loop().run_until_complete(simple_message_example())
