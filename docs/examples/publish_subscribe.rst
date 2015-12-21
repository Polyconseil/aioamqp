Publish Subscribe : Sending messages to many consumers at once
==============================================================

This part of the tutorial introduce `exchange`.

A `emit_log.py` scripts publish messages into a `fanout` exchange.
Then the `receive_log.py` script creates a temporary queue (which is deleted on the disconnection).

If the script `receive_log.py` is ran multiple times, all the instance will receive the message emitted by `emit_log`.


Publisher
---------

The publisher create a new `fanout` exchange:

 .. code-block:: python

    yield from channel.exchange_declare(exchange_name='logs', type_name='fanout'


And publish message into that exchange:

 .. code-block:: python

    yield from channel.basic_publish(message, exchange_name='logs', routing_key='')

Consumer
--------

The consumer create a temporary queue and binds it to the exchange.

 .. code-block:: python

    yield from channel.exchange(exchange_name='logs', type_name='fanout')
    # let RabbitMQ generate a random queue name
    result = yield from channel.queue(queue_name='', exclusive=True)

    queue_name = result['queue']
    yield from channel.queue_bind(exchange_name='logs', queue_name=queue_name, routing_key='')


