#!/usr/bin/env python

import pika


def callback(ch, method, properties, body):
    print(" [x] Received %r" % (body,))

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


queue_name = "py2.queue"
channel.queue_declare(queue=queue_name)


channel.basic_consume(callback, queue=queue_name, no_ack=True)

channel.start_consuming()
