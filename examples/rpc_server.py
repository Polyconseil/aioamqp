"""
    RPC server, aioamqp implementation of RPC examples from RabbitMQ tutorial
"""

import asyncio
import aioamqp


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n-1) + fib(n-2)


@asyncio.coroutine
def rpc_server():

    transport, protocol = yield from aioamqp.connect()

    channel = yield from protocol.channel()

    yield from channel.queue_declare(queue_name='rpc_queue')
    yield from channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)
    consumer = yield from channel.basic_consume(queue_name='rpc_queue')
    print(" [x] Awaiting RPC requests")

    while (yield from consumer.fetch_message()):
        channel, body, envelope, properties = consumer.get_message()
        n = int(body)

        print(" [.] fib(%s)" % n)
        response = fib(n)

        yield from channel.basic_publish(
            payload=str(response),
            exchange_name='',
            routing_key=properties.reply_to,
            properties={
                'correlation_id': properties.correlation_id,
            },
        )

        yield from channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(rpc_server())
event_loop.run_forever()


