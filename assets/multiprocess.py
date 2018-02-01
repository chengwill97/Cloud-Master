import multiprocessing
import time
import csv
import os
import pickle
import pika
import random

from math import pow

from nodes import SimulationNode
from nodes import AnalysisNode
from nodes import ConvergenceNode

from dataIO import read_json
from dataIO import write_json
from dataIO import append_csv

from pipeline import Pipeline


def pop_remaining(channel, remaining_queue):

	url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost/%2f')
	params = pika.URLParameters(url)
	params.socket_timeout = 5
	connection = pika.BlockingConnection(params) # Connect to CloudAMQP	

	channel = connection.channel()
	method_frame, header_frame, body = channel.basic_get(remaining_queue)
	if method_frame:
	    # print method_frame, header_frame, body
	    channel.basic_ack(method_frame.delivery_tag)
	else:
	    print ' [x] No message returned'


#######################################################################
#
# 	worker Function
#
# 	Worker that emaulates the Expanded Ensemble
# 	Simulate -> Analyze -> Converge sequence
#
def worker_sleeps(ch, method, properties, body):

	# Unpickle data
	unpickle = pickle.loads(body)

	data = unpickle['data']
	count_queue = unpickle['count_queue']

	sleep_time, run_dir = data
	process_name   = multiprocessing.current_process().name
	pipe_dir = run_dir + '/' + process_name

	# Create Nodes for Expanded Ensemble algorithm
	simulation_node 	= SimulationNode(sleep_time=sleep_time, process_name=process_name)
	analysis_node   	= AnalysisNode(sleep_time=sleep_time, process_name=process_name)
	convergence_node	= ConvergenceNode(sleep_time=sleep_time, process_name=process_name)

	# Find available directory name
	process_num = 0
	worker_dir = pipe_dir + '_' + "%03d" % process_num
	while(not os.path.exists(worker_dir)):
		try:
			os.mkdir(worker_dir)
		except (OSError, IOError) as e:
			process_num += 1
			worker_dir = pipe_dir + '_' + "%03d" % process_num
			continue
	
	pipeline = Pipeline(simulation=simulation_node,
						analysis=analysis_node,
						convergence=convergence_node)

	# Execute nodes
	results = pipeline.run()
	
	# Write results into json file
	write_json(worker_dir + '/results.json', results)

	ch.basic_ack(delivery_tag=method.delivery_tag)

	pop_remaining(ch, count_queue)


#######################################################################
#
# 	hire_worker Function
#
# 	hire process that executes tasks
#
def hire_worker(queue_name):

	process_name = multiprocessing.current_process().name
	# Establish connection with the RabbitMQ server
	url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost/%2f')
	params = pika.URLParameters(url)
	params.socket_timeout = 5
	connection = pika.BlockingConnection(params) # Connect to CloudAMQP
	channel = connection.channel()

	channel.queue_declare(queue=queue_name, durable=True)

	# Sets maximum number of pre-assigned tasks to 1
	channel.basic_qos(prefetch_count=1)

	channel.basic_consume(worker_sleeps,
						  queue=queue_name)

	print(' [x] %s Waiting for messages. To exit press CTRL+C' % process_name)

	try:
		channel.start_consuming()
	except KeyboardInterrupt:
		connection.close()


#######################################################################
#
# 	single_run Function
#
# 	Does a single test with the current parameteres
#
def get_queue_depth(ch, queue_name):

	return ch.queue_declare(queue=queue_name, passive=True).method.message_count


#######################################################################
#
# 	single_run Function
#
# 	Does a single test with the current parameteres
#
def single_run(test_dir, message_server, single_run_parameters, max_cores):

	NUMBER_CORES 	= single_run_parameters['number_cores']
	NUMBER_JOBS		= single_run_parameters['number_jobs']

	total_number_jobs = int(pow(2, NUMBER_JOBS))
	number_cores = int(pow(2, NUMBER_CORES))

	task_queue 		= message_server['task_queue']
	count_queue  	= message_server['count_queue']

	print ' [x] Starting Single Run with %02d cores' % total_number_jobs

	# Check that parameters are valid
	if NUMBER_CORES > max_cores or NUMBER_CORES < 0:
		print ' [x] Error: Number of cores is not valid'
		return

	run_dir = get_single_run_dir(test_dir)

	# workers are how many cores are to be used
	workers = int(pow(2, NUMBER_CORES))

	# Map tasks to processes
	pool = multiprocessing.Pool(processes=workers)

	# Injects each tasks into a function asynchronously
	for worker in xrange(workers):
		pool.apply_async(hire_worker,[task_queue])
		print ' [x] Worker #%02d hired' % worker
	
	'''
	Set the connection parameters to connect to 'server' on port 5672
	on the / virtual host using the username "guest" and password "guest"
	and establish a connection with RabbitMQ server
	'''
	url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost/%2f')
	params = pika.URLParameters(url)
	params.socket_timeout = 5
	connection = pika.BlockingConnection(params) # Connect to CloudAMQP

	# credentials = pika.PlainCredentials('cloud_user', 'cloud_password')
	# connection_parameters = pika.ConnectionParameters(server,
	#                                        5672,
	#                                        '/',
	#                                        credentials)
	# connection = pika.BlockingConnection(connection_parameters)

	channel = connection.channel()

	# Declare queue to be used in the input transfer process
	channel.queue_declare(queue=task_queue, durable=True)

	# Declare queue to be used to count number of remaining running jobs
	channel.queue_declare(queue=count_queue, durable=True)

	try:

		'''
		This loop iterates through the tasks 
		and loads a task into the task_queue
		for a worker to complete the task.
		The maximum number of tasks that is 
		allowed to be loaded into the queue
		is equal to the number of cores set.
		'''
		task_count = 0
		for i in xrange(total_number_jobs):

			for j in xrange(number_cores):

				print ' [x] tasks remaining %s %d' % (
					40*'.', total_number_jobs - task_count)

				task_count += 1

				sleep_time 	= 1
				task 		= (sleep_time, run_dir)
				
				# Pickle task
				pickled_task = pickle.dumps(
					{'data': task,
					 'count_queue' : count_queue
					})

				# Upload task parameters to queue
				channel.basic_publish(exchange='',
					  routing_key=task_queue,
					  body=pickled_task,
					  properties=pika.BasicProperties(
						delivery_mode = 2, # make message persistent
					  ))

				# Increase number of items in queue
				channel.basic_publish(exchange='',
					  routing_key=count_queue,
					  body='',
					  properties=pika.BasicProperties(
						delivery_mode = 2, # make message persistent
					  ))

				if total_number_jobs <= task_count:
					break

			begin_time = time.time()

			# Waits until all tasks are completed
			while get_queue_depth(channel, task_queue) + get_queue_depth(channel, count_queue) > 0:
				# print "task_queue length: %d\ncount_queue length:%d" % (get_queue_depth(channel, task_queue), get_queue_depth(channel, count_queue))
				time.sleep(0.1)

				"""
				#FIXME
				Check if queue is timing out. If it is, 
				then purge queues and go to next experiment
				"""
				sleep_time = 1
				end_time = time.time()
				if end_time - begin_time >= 60:
					print ' [x] Exiting due to queue timeout'
					print ' [x] in run with %d number cores and %d total number of jobs' % (number_cores, total_number_jobs)
					print ' [x] \nQueues (%s, %s) purging with (%d, %d) items in queue' % (
						task_queue, 
						count_queue,
						get_queue_depth(channel, task_queue), 
						get_queue_depth(channel, count_queue))
					channel.queue_purge(queue=count_queue)
					channel.queue_purge(queue=task_queue)
					# channel.queue_delete(queue=count_queue)
					# channel.queue_delete(queue=task_queue)
					connection.close()
					pool.terminate()
					pool.join()
					
					return 'Error: Queue Timeout Error'


			if total_number_jobs <= task_count:
				break

	except KeyboardInterrupt:

		print ' [x] Exiting due to KeyboardInterrupt exception'
		print ' [x] in run with %d number cores and %d total number of jobs' % (number_cores, total_number_jobs)
		print ' [x] \nQueues (%s, %s) purging with (%d, %d) items in queue' % (
			task_queue, 
			count_queue,
			get_queue_depth(channel, task_queue), 
			get_queue_depth(channel, count_queue))

		# Reset queues
		channel.queue_purge(queue=count_queue)
		channel.queue_purge(queue=task_queue)

		connection.close()
		pool.terminate()
		pool.join()

		return 'Error: KeyboardInterrupt'

	connection.close()
	pool.terminate()
	pool.join()		
	return run_dir


#######################################################################
#
# 	weak_scale_run Function
#
# 	Does a weak scale test with the current weak scale parameters
#
def weak_scale_run(test_dir, message_server, weak_scale_parameters, max_cores):

	print ' [x] Starting Weak Scale Run:\n'

	BEGIN_NUMBER_CORES 	= weak_scale_parameters['begin_number_cores']
	END_NUMBER_CORES 	= weak_scale_parameters['end_number_cores'] + 1
	BEGIN_JOBS_PER_CORE = weak_scale_parameters['begin_jobs_per_core']
	END_JOBS_PER_CORE 	= weak_scale_parameters['end_jobs_per_core'] + 1

	# Check that parameters are valid
	if BEGIN_NUMBER_CORES > END_NUMBER_CORES or BEGIN_NUMBER_CORES < 0:
		print ' [x] Error: bounds on cores are not valid.'
		return
	elif BEGIN_JOBS_PER_CORE > END_JOBS_PER_CORE:
		print ' [x] Error: jobs_per_core are not valid.'
		return
	elif END_NUMBER_CORES > max_cores:
		print ' [x] Error: cores exceed max_cores.'
		return

	weak_scale_test_dir = get_weak_scale_test_dir(test_dir)

	csv_file = '%s/data.csv' % weak_scale_test_dir
	header = ['Test Directory', '2 ^ Number of Cores', '2 ^ Jobs per Cores', 'Run Time']
	append_csv(csv_file, header)

	# Vary the number of cores
	for number_cores in xrange(BEGIN_NUMBER_CORES, END_NUMBER_CORES):

		print ' [x] Running weak scale with %d cores' % pow(2, number_cores)
		
		# Vary the number of jobs per core
		for jobs_per_core in xrange(BEGIN_JOBS_PER_CORE, END_JOBS_PER_CORE):

			print '\t [x] Running weak scale with %d jobs per core' % pow(2,  jobs_per_core)

			single_run_parameters = dict()

			single_run_parameters['number_cores'] 	= number_cores
			single_run_parameters['number_jobs']	= number_cores * jobs_per_core 

			# Start timer
			begin_time = time.time()

			########################################################

			single_run_dir = single_run(weak_scale_test_dir,
				message_server,
				single_run_parameters,
				max_cores)

			###############################################################

			# End timer
			end_time = time.time()
			run_time = end_time - begin_time

			data = [single_run_dir, pow(2, number_cores), pow(2, jobs_per_core), run_time]

			# Export data into csv_file
			append_csv(csv_file, data)


#######################################################################
#
# 	strong_scale_run Function
#
# 	Does a strong scale test with the current strong scale parameters
#
def strong_scale_run(test_dir, message_server, strong_scale_parameters, max_cores):

	print 'Starting Strong Scale Run:\n'

	BEGIN_NUMBER_JOBS 	= strong_scale_parameters['begin_number_jobs']
	END_NUMBER_JOBS 	= strong_scale_parameters['end_number_jobs']
	BEGIN_NUMBER_CORES 	= strong_scale_parameters['begin_number_cores']
	END_NUMBER_CORES 	= strong_scale_parameters['end_number_cores']

	# Check that parameters are valid
	if BEGIN_NUMBER_CORES == END_NUMBER_CORES:
		END_NUMBER_CORES += 1
	if BEGIN_NUMBER_JOBS == END_NUMBER_JOBS:
		END_NUMBER_JOBS += 1

	# Check that parameters are valid
	if BEGIN_NUMBER_JOBS > END_NUMBER_JOBS or BEGIN_NUMBER_JOBS < 0:
		print ' [x] Error: bounds on cores are not valid.'
		return
	elif BEGIN_NUMBER_CORES > END_NUMBER_CORES or BEGIN_NUMBER_CORES < 0:
		print ' [x] Error: number_cores are not valid.'
		return
	elif END_NUMBER_CORES > max_cores:
		print ' [x] Error: cores exceed max_cores.'
		return

	strong_scale_test_dir = get_strong_scale_test_dir(test_dir)

	csv_file = '%s/data.csv' % strong_scale_test_dir
	header = ['Test Directory', '2 ^ Number of Jobs', '2 ^ Number of Cores', 'Run Time']
	append_csv(csv_file, header)

	# Vary the number of jobs
	for number_jobs in xrange(BEGIN_NUMBER_JOBS, END_NUMBER_JOBS):

		print ' [x] Running strong scale with %d jobs' % pow(2, number_jobs)
		
		# Vary the number of cores
		for number_cores in xrange(BEGIN_NUMBER_CORES, END_NUMBER_CORES):

			print ' [x] Running strong scale with %d cores' % pow(2,  number_cores)

			single_run_parameters = dict()

			single_run_parameters['number_cores'] 	= number_cores
			single_run_parameters['number_jobs']	= number_jobs

			# Start timer
			begin_time = time.time()

			###############################################################

			single_run_dir = single_run(strong_scale_test_dir,
				message_server,
				single_run_parameters,
				max_cores)

			###############################################################

			# End timer
			end_time = time.time()
			run_time = end_time - begin_time

			data = [single_run_dir, pow(2, number_jobs), pow(2, number_cores), run_time]

			# Export data into csv_file
			append_csv(csv_file, data)


#######################################################################
#
# 	get_singel_run_dir Function
#
# 	Returns the output for the single run
#
def get_single_run_dir(test_dir):

	single_run_dir_num = 0

	single_run_dir = '%s/single_run_output%03d' % (test_dir, single_run_dir_num)
	while (os.path.exists(single_run_dir)):
		os.path.exists(single_run_dir)
		try:
			os.mkdir(single_run_dir)
		except (OSError, IOError) as e:
			single_run_dir_num += 1
			single_run_dir = '%s/single_run_output%03d' % (test_dir, single_run_dir_num)

	# Check if single_run_dir exists
	try:
		os.mkdir(single_run_dir)
	except (OSError, IOError) as e:
		print ' [x] Single run test directory already exists'
		exit(0)

	return single_run_dir


#######################################################################
#
# 	get_weak_scale_test_dir Function
#
# 	Returns the output for the weak scale test
def get_weak_scale_test_dir(test_dir):

	weak_scale_test_dir = test_dir + '/weak_scale_output'

	# Check if weak_scale_test_dir exists
	try:
		os.mkdir(weak_scale_test_dir)
	except (OSError, IOError) as e:
		print ' [x] Weak scale test directory already exists'
		exit(0)

	return weak_scale_test_dir


#######################################################################
#
# 	get_strong_scale_test_dir Function
#
# 	Returns the output for the strong scale test
#
def get_strong_scale_test_dir(test_dir):

	strong_scale_test_dir = test_dir + '/strong_scale_output'

	# Check if strong_scale_test_dir exists
	try:
		os.mkdir(strong_scale_test_dir)
	except (OSError, IOError) as e:
		print ' [x] Strong scale test directory already exists'
		exit(0)

	return strong_scale_test_dir

#######################################################################
#
# 	assign_tasks Function
#
# 	populate cores with pipelines
#
def assign_tasks(number_cores, worker):

	workers = int(pow(2, number_cores))

	# Map tasks to processes
	pool = multiprocessing.Pool(processes=workers)

	# Injects each tasks into a function asynchronously
	for worker in xrange(workers):
		pool.apply_async(hire_worker)

	try:
		while True:
			continue
	except KeyboardInterrupt:
		print ' [x] Exiting'
		pool.terminate()
		pool.join()

