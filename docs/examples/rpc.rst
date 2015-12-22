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
