import pika

import multiprocessing

import time

import pickle

def send_task(message): 
	# Establish a connection with RabbitMQ server
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
	channel = connection.channel()

	# creating a 'hello' queue to which message will be delivered
	channel.queue_declare(queue='task_queue', durable=True)

	# message needs to go through exchange
	channel.basic_publish(exchange='',
						  routing_key='task_queue',
						  body=message,
						  properties=pika.BasicProperties(
						  	delivery_mode = 2, # make message persistent
						  ))

	print(" [x] Sent %r" % message)

	connection.close()


def callback(ch, method, properties, body):
	process_name   = multiprocessing.current_process().name

	body = pickle.loads(body)	

	print(" [x] %s Received %r" % (body,process_name))

	# time.sleep(body.count(b'.'))
	time.sleep(1)

	print(" [x] Done")

	ch.basic_ack(delivery_tag=method.delivery_tag)


def hire_worker():

	process_name   = multiprocessing.current_process().name

	# Establish connection with the RabbitMQ server
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

	channel = connection.channel()

	channel.queue_declare(queue='task_queue', durable=True)

	# Sets maximum number of pre-assigned tasks to 1
	channel.basic_qos(prefetch_count=1)

	channel.basic_consume(callback,
						  queue='task_queue')

	print(' [*] %s Waiting for messages. To exit press CTRL+C' % process_name)

	try:
		channel.start_consuming()
	except KeyboardInterrupt:
		pass

if __name__ == '__main__':

	workers = 2

	pool = multiprocessing.Pool(processes=4)

	for worker in xrange(0,workers):
		pool.apply_async(hire_worker)
		print "process started"

	try:
		while True:
			continue
	except KeyboardInterrupt:
		print ' [x] Exiting'
		pool.terminate()
		pool.join()
