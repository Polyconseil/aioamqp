#!/usr/bin/env python

"""
    Amqp client exemple

"""
import asyncio
import aioamqp


@asyncio.coroutine
def hello():
    protocol = yield from aioamqp.connect('localhost', 5672)

    try:
        yield from protocol.start_connection()
    except aioamqp.ClosedConnection:
        print("closed connections")
        return

    channel = protocol.channel()
    # channel = yield from cnx.channel('name', 'fanout')
    # yield from channel.publish(message)
    # print("publish")

    #yield from protocol.channel("/")

asyncio.get_event_loop().run_until_complete(hello())
