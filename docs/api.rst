API
===

.. module:: aioamqp
    :synopsis: public Jinja2 API


Basics
------

There are two principal objects when using aioamqp:

 * The protocol object, used to begin a connection to aioamqp,
 * The channel object, used when creating a new channel to effectively use an AMQP channel.


Starting a connection
---------------------

Starting a connection to AMQP really mean instanciate a new asyncio Protocol subclass::

    import asyncio
    import aioamqp

    @asyncio.coroutine
    def connect():
        protocol = yield from aioamqp.connect('localhost', 5672)

        try:
            yield from protocol.start_connection()
        except aioamqp.ClosedConnection:
            print("closed connections")
            return

        yield from asyncio.wait_for(protocol.client_close(), timeout=10)

    asyncio.get_event_loop().run_until_complete(connect())

In this example, we just use the method "start_connection" to begin a communication with the server, which deals with credentials and connection tunning.


Publishing messages
-------------------

A channel is the main object when you want to send message to an exchange, or to consume message from a queue::

    channel = yield from protocol.channel()


When you want to produce some content, you declare a queue then publish message into it::

    queue = yield from channel.queue("my_queue")
    yield from queue.publish("aioamqp hello", '', "my_queue")

Note: we're pushing message to "my_queue" queue, through the default amqp exchange.


Consuming messages
------------------

When consuming message, you connect to the same queue you previously created. But you *must* create the queue with the same configuration::

    channel = yield from protocol.channel()
    queue = yield from channel.queue("my_queue")
    
Then, we start consuming messages::

    yield from channel.basic_consume("my_queue")
    while True:
        consumer_tag, delivery_tag, payload = yield from channel.consume()
        print(payload) 

The ``basic_consume`` method tells the server to send us the messages, and ``consume`` effectively unqueue a message.

The ``consumer_tag`` is the AMQP worker that unqueued the message, and the ``delivery_tag`` is the tag used if you want to acknowledge it.

Using exchanges
---------------

You can bind an exchange to a queue::

    channel = yield from protocol.channel()
    exchange = yield from channel.exchange_declare(exchange_name="my_exchange", type_name='fanout') 
    queue = yield from channel.queue("my_queue")
    yield from channel.queue_bind("my_queue", "my_exchange")

