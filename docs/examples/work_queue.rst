Work Queues : Distributing tasks among workers
==============================================

The main purpose of this part of the tutorial is to `ack` a message in RabbitMQ only when it's really processed by a worker.

new_task
--------

This publisher creates a queue with the `durable` flag and publish a message with the property `presistent`.

 .. code-block:: python

    yield from channel.queue('task_queue', durable=True)

    yield from channel.basic_publish(
        payload=message,
        exchange_name='',
        routing_key='task_queue',
        properties={
            'delivery_mode': 2,
        },
    )


worker
------

The purpose of this worker is to simulate a resource consuming execution which delays the processing of the other messages.

The worker declares the queue with the exact same argument of the `new_task` producer.

 .. code-block:: python

    yield from channel.queue('task_queue', durable=True)


Then, the worker configure the `QOS`: it specifies how the worker unqueues message.

 .. code-block:: python

    yield from channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)


Finaly we have to create a callback that will `ack` the message to mark it as `processed`.
Note: the code in the callback calls `asyncio.sleep` to simulate an asyncio compatible task that takes time.
You probably want to block the eventloop to simulate a CPU intensive task using `time.sleep`.

 .. code-block:: python

    @asyncio.coroutine
    def callback(channel, body, envelope, properties):
        print(" [x] Received %r" % body)
        yield from asyncio.sleep(body.count(b'.'))
        print(" [x] Done")
        yield from channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

