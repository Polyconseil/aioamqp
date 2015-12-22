RPC: Remote procedure call implementation
=========================================


This tutorial will try to implement the RPC as in the RabbitMQ's tutorial.

The API will probably look like:

 .. code-block:: python

     fibonacci_rpc = FibonacciRpcClient()
     result = yield from fibonacci_rpc.call(4)
     print("fib(4) is %r" % result)


Client
------

In this case it's no more a producer but a Client: we will send a message in a queue and wait for a response in another.
For that purpose, we publish a message to the `rpc_queue` and add a `reply_to` properties to let the server know where to respond.

 .. code-block:: python

    result = yield from channel.queue_declare(exclusive=True)
    callback_queue = result['queue']

    channel.basic_publish(
        exchange='',
        routing_key='rpc_queue',
        properties={
            'reply_to': callback_queue,
        },
        body=request,
    )


Note: the client use a `waiter` (an asyncio.Event) which will be set when receiving a response from the previously sent message.


Server
------

When unqueing a message, the server will publish a response directly in the callback. The `correlation_id` is used to let the client know it's a response from this request.

 .. code-block:: python

    @asyncio.coroutine
    def on_request(channel, body, envelope, properties):
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

