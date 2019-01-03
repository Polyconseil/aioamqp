"""
    Hello world `send.py` example implementation using aioamqp.
    See the documentation for more informations.

    If there is no queue listening for the routing key, the message will
    get returned.

"""

import asyncio
import aioamqp


async def handle_return(channel, body, envelope, properties):
    print('Got a returned message with routing key: {}.\n'
          'Return code: {}\n'
          'Return message: {}\n'
          'exchange: {}'.format(envelope.routing_key, envelope.reply_code,
                                envelope.reply_text, envelope.exchange_name))


async def send():
    transport, protocol = await aioamqp.connect()
    channel = await protocol.channel(return_callback=handle_return)

    await channel.queue_declare(queue_name='hello')

    await channel.basic_publish(
        payload='Hello World!',
        exchange_name='',
        routing_key='helo',  # typo on purpose, will cause the return
        mandatory=True,
    )

    print(" [x] Sent 'Hello World!'")
    await protocol.close()
    transport.close()


asyncio.get_event_loop().run_until_complete(send())
