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
        try:
            transport, protocol = yield from aioamqp.connect()  # use default parameters
        except aioamqp.AmqpClosedConnection:
            print("closed connections")
            return

        print("connected !")
        yield from asyncio.sleep(1)

        print("close connection")
        yield from transport.close()

    asyncio.get_event_loop().run_until_complete(connect())

In this example, we just use the method "start_connection" to begin a communication with the server, which deals with credentials and connection tunning.


Publishing messages
-------------------

A channel is the main object when you want to send message to an exchange, or to consume message from a queue::

    channel = yield from protocol.channel()


When you want to produce some content, you declare a queue then publish message into it::

    queue = yield from channel.queue_declare("my_queue")
    yield from queue.publish("aioamqp hello", '', "my_queue")

Note: we're pushing message to "my_queue" queue, through the default amqp exchange.


Consuming messages
------------------

When consuming message, you connect to the same queue you previously created::

    import asyncio
    import aioamqp

    @asyncio.coroutine
    def callback(body, envelope, properties):
        print(body)

    channel = yield from protocol.channel()
    yield from channel.basic_consume("my_queue", callback=callback)

The ``basic_consume`` method tells the server to send us the messages, and will call ``callback`` with amqp response arguments.

The ``consumer_tag`` is the id of your consumer, and the ``delivery_tag`` is the tag used if you want to acknowledge the message.

In the callback:

* the first ``body`` parameter is the message
* the ``envelope`` is an instance of envelope.Envelope class which encapsulate a group of amqp parameter such as::

    consumer_tag
    delivery_tag
    exchange_name
    routing_key
    is_redeliver

* the ``properties`` are message properties, an instance of properties.Properties with the following members::

    content_type
    content_encoding
    headers
    delivery_mode
    priority
    correlation_id
    reply_to
    expiration
    message_id
    timestamp
    type
    user_id
    app_id
    cluster_id


Using exchanges
---------------

You can bind an exchange to a queue::

    channel = yield from protocol.channel()
    exchange = yield from channel.exchange_declare(exchange_name="my_exchange", type_name='fanout')
    yield from channel.queue_declare("my_queue")
    yield from channel.queue_bind("my_queue", "my_exchange")

