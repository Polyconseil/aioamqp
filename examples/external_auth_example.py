#!/usr/bin/env python
"""
RabbitMQ publisher
"""
import asyncio
import aioamqp
import json


@asyncio.coroutine
def receive():
    ssl_options = {
        'certfile': 'client.pem',
        'keyfile': 'client.key',
        'ca_certs': 'cacert.pem',
    }
    transport, protocol = yield from aioamqp.connect(
        host='host',
        login="login",
        ssl=True,
        ssl_options=ssl_options,
        login_method='EXTERNAL'
    )
    channel = yield from protocol.channel()

    yield from channel.basic_publish(
        payload=json.dumps({'xxx': 'Hello World!'}),
        exchange_name='honeypots',
        routing_key='dionaea'
    )

event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive())
event_loop.run_forever()
