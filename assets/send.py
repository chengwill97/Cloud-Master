import pika

rabbitmq_server = 'localhost'
connection_parameters = pika.ConnectionParameters(rabbitmq_server)
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()

channel.queue_declare(queue='test', durable=True)

channel.basic_publish(exchange='',
  routing_key='test',
  body='job',
  properties=pika.BasicProperties(
	delivery_mode = 2, # make message persistent
  ))

connection.close()