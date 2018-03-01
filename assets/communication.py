import pika
import os

#######################################################################
#
# 	pop_remaining Function
#
# 	decreases the remaining
#
def pop_remaining(channel, remaining_queue):

	result = channel.basic_get(remaining_queue)

	if result:
		method_frame, header_frame, body = result
		if method_frame:
			channel.basic_ack(method_frame.delivery_tag)
	else:
		print ' [x] No message returned'

#######################################################################
#
# 	Communication class takes care of administering the communication
#	with the RabbitMQ server
#
#
class Communication:

	def __init__(self):

		self.connection = self.connection()
		self.channel 	= self.connection.channel()
		self.queues 	= list()


	def connection(self, socket_timeout=5):
		'''
		Set the connection parameters to connect to 'server' on port 5672
		on the / virtual host using the username "guest" and password "guest"
		and establish a connection with RabbitMQ server
		'''
		url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost/%2f')
		params = pika.URLParameters(url)
		params.socket_timeout = socket_timeout
		connection = pika.BlockingConnection(params) # Connect to CloudAMQP

		return connection


	# Create a queue
	def queue_start(self, queue_name, durable, purge=False):
		# Declare queue to be started up
		self.channel.queue_declare(queue=queue_name, durable=durable)
		# Refresh queue if called
		if purge:
			self.channel.queue_purge(queue=queue_name)

		self.queues.append(queue_name)

		return


	# Obtain queue depth
	def queue_depth(self, queue_names):
		try:
			message_counts = list()
			for queue_name in queue_names:
				message_counts.append(
					self.channel.queue_declare(queue=queue_name, passive=True).method.message_count
					)

			return message_counts
		except Exception as e:
			print e
			exit(0)


	# Print queue name and corresponding message count
	def print_queue_info(self):

		message_counts = self.queue_depth(self.queues)
		i = 0

		print ' [x] Purging...'
		for queue in self.queues:
			print 'queue %s: %d messages' % (queue, message_counts[i])
			i += 1

		return


	# Purge all queues created through this channel
	def purge_all_queues(self):

		for queue in self.queues: 
			self.channel.queue_purge(queue=queue)

		return
