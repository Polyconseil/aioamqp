"Hello World!" : The simplest thing that does something
=======================================================

Sending
-------

Our first script to send a single message to the queue.

Creating a new connection:

 .. code-block:: python

    import asyncio
    import aioamqp

    @asyncio.coroutine
    def connect():
        transport, protocol = yield from aioamqp.connect()
        channel = yield from protocol.channel()

    asyncio.get_event_loop().run_until_complete(connect())


This first scripts shows how to create a new connection to the `AMQP` broker.

Now we have to declare a new queue to receive our messages:

 .. code-block:: python

    yield from channel.queue_declare(queue_name='hello')

We're now ready to publish message on to this queue:

 .. code-block:: python

    yield from channel.basic_publish(
        payload='Hello World!',
        exchange_name='',
        routing_key='hello'
    )


We can now close the connection to rabbit:

 .. code-block:: python

    # close using the `AMQP` protocol
    yield from protocol.close()
    # ensure the socket is closed.
    transport.close()

You can see the full example in the file `example/send.py`.

Receiving
---------

We now want to unqueue the message in the consumer side.

We have to ensure the queue is created. Queue declaration is indempotant.

 .. code-block:: python

    yield from channel.queue_declare(queue_name='hello')


To consume a message is used the Consumer object using an async iter if is python 3.5 or while with ``fetch_message`` and ``get_message`` if is python < 3.5:

 .. code-block:: python

    consumer = yield from channel.basic_consume(queue_name='hello', no_ack=True)

    while (yield from consumer.fetch_message()):
        channel, body, envelope, properties = consumer.get_message()
        print(body)

For python 3.5 :

.. code-block:: python

    consumer = await channel.basic_consume(queue_name='hello', no_ack=True)

    async for channel, body, envelope, properties in consumer:
        print(body)
