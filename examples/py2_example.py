#!/usr/bin/env python

"""
 The default exchange is implicitly bound to every queue,
 with a routing key equal to the queue name.

 It it not possible to explicitly bind to, or unbind from the default exchange.
 It also cannot be deleted.
"""

import time

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


queue_name = "py2.queue"
channel.queue_declare(queue=queue_name)


while True:
    channel.basic_publish(exchange='', routing_key='py2.queue', body='py2.message')

#import ipdb; ipdb.set_trace()
connection.close()
