Routing : Receiving messages selectively
========================================


Routing is an interesting concept in RabbitMQ/AMQP: in this tutorial, messages are published to a `direct` exchange with a specific routing_key (the log `severity` 
The `consumer` create a queue, binds the queue to the exchange and specifies the severity he wants to receive.


Publisher
---------

The publisher creater the `direct` exchange:

 .. code-block:: python

    yield from channel.exchange(exchange_name='direct_logs', type_name='direct')


Message are published into that exchange and routed using the severity for instance:

 .. code-block:: python

    yield from channel.publish(message, exchange_name='direct_logs', routing_key='info')


Consumer
--------

The consumer may subscribe to multiple severities. To accomplish this purpose, it create a queue bind this queue multiple time using the `(exchange_name, routing_key)` configuration:

 .. code-block:: python

    result = yield from channel.queue(queue_name='', durable=False, auto_delete=True)

    queue_name = result['queue']

    severities = sys.argv[1:]
    if not severities:
        print("Usage: %s [info] [warning] [error]" % (sys.argv[0],))
        sys.exit(1)

    for severity in severities:
        yield from channel.queue_bind(
            exchange_name='direct_logs',
            queue_name=queue_name,
            routing_key=severity,
        )
