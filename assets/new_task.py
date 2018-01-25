#!/usr/bin/env python2.7

import pika

import sys

import pickle


"""
Our program will sened a single message to the queue
"""

# Establish a connection with RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# creating a 'hello' queue to which message will be delivered
channel.queue_declare(queue='task_queue', durable=True)

# Send string 'Hello World!' to hello queue
# message needs to go through exchange
message = ' '.join(sys.argv[1:]) or "Hello World!"
message = dict()

stream = pickle.dumps(message)

channel.basic_publish(exchange='',
					  routing_key='task_queue',
					  body=stream,
					  properties=pika.BasicProperties(
					  	delivery_mode = 2, # make message persistent
					  ))

print(" [x] Sent %r" % message)

connection.close()