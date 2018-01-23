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


def pop_remaining(remaining_queue):

	connection = pika.BlockingConnection()
	channel = connection.channel()
	method_frame, header_frame, body = channel.basic_get(remaining_queue)
	if method_frame:
	    # print method_frame, header_frame, body
	    channel.basic_ack(method_frame.delivery_tag)
	else:
	    print 'No message returned'


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

	pop_remaining(count_queue)


#######################################################################
#
# 	hire_worker Function
#
# 	hire process that executes tasks
#
def hire_worker(queue_name):

	process_name = multiprocessing.current_process().name

	# Establish connection with the RabbitMQ server
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

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

	server 			= message_server['server']
	task_queue 		= message_server['task_queue']
	count_queue  	= message_server['count_queue']

	print '\nStarting Single Run with %02d cores' % NUMBER_CORES

	# Check that parameters are valid
	if NUMBER_CORES > max_cores or NUMBER_CORES < 0:
		print 'Error: Number of cores is not valid'
		return

	run_dir = get_single_run_dir(test_dir)

	# workers are how many cores are to be used
	workers = int(pow(2, NUMBER_CORES))

	# Map tasks to processes
	pool = multiprocessing.Pool(processes=workers)

	# Injects each tasks into a function asynchronously
	for worker in xrange(workers):
		pool.apply_async(hire_worker,[task_queue])
		print 'Worker #%02d hired' % worker

	# Establish a connection with RabbitMQ server
	connection = pika.BlockingConnection(pika.ConnectionParameters(server))
	channel = connection.channel()

	# Declare queue to be used in the transfer process
	channel.queue_declare(queue=task_queue, durable=True)

	# Declare queue to be used to count number of remaining jobs
	channel.queue_declare(queue=count_queue, durable=True)

	total_number_tasks = int(pow(2, NUMBER_JOBS))
	number_cores = int(pow(2, NUMBER_CORES))

	try:

		'''
		This loop iterates through the tasks 
		and loads a task into the task_queue
		for a worker to complete the task.
		The maximum number of tasks that is 
		allowed to be loaded into the queue
		is equal to the number of cores set.
		'''

		for i in xrange(total_number_tasks):

			for j in xrange(number_cores):

				i += 1

				sleep_time 	= 0.1
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

			# Waits until all tasks are completed
			while get_queue_depth(channel, count_queue) > 0:
				time.sleep(0.1)

	except KeyboardInterrupt:
		connection.close()
		pool.terminate()
		pool.join()

	print ' [x] Exiting gracefully...'
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

	print 'Starting Weak Scale Run:\n'

	BEGIN_NUMBER_CORES 	= weak_scale_parameters['begin_number_cores']
	END_NUMBER_CORES 	= weak_scale_parameters['end_number_cores'] + 1
	BEGIN_JOBS_PER_CORE = weak_scale_parameters['begin_jobs_per_core']
	END_JOBS_PER_CORE 	= weak_scale_parameters['end_jobs_per_core'] + 1

	# Check that parameters are valid
	if BEGIN_NUMBER_CORES > END_NUMBER_CORES or BEGIN_NUMBER_CORES < 0:
		print 'Error: bounds on cores are not valid.'
		return
	elif BEGIN_JOBS_PER_CORE > END_JOBS_PER_CORE:
		print 'Error: jobs_per_core are not valid.'
		return
	elif END_NUMBER_CORES > max_cores:
		print 'Error: cores exceed max_cores.'
		return

	weak_scale_test_dir = get_weak_scale_test_dir(test_dir)

	# Vary the number of cores
	for number_cores in xrange(BEGIN_NUMBER_CORES, END_NUMBER_CORES):

		print 'Running weak scale with %d cores' % pow(2, number_cores)
		
		# Vary the number of jobs per core
		for jobs_per_core in xrange(BEGIN_JOBS_PER_CORE, END_JOBS_PER_CORE):

			print '\tRunning weak scale with %d jobs per core' % pow(2,  jobs_per_core)

			single_run_parameters = dict()

			single_run_parameters['number_cores'] 	= number_cores
			single_run_parameters['number_jobs']	= number_cores * jobs_per_core 

			# Start timer
			begin_time = time.time()

			########################################################

			run_dir = single_run(weak_scale_test_dir,
				message_server,
				single_run_parameters,
				max_cores)

			###############################################################

			# End timer
			end_time = time.time()
			run_time = end_time - begin_time

			csv_file = '%s/data_.csv' % run_dir
			data = [pow(2, number_cores), pow(2, jobs_per_core), run_time]

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
		print 'Error: bounds on cores are not valid.'
		return
	elif BEGIN_NUMBER_CORES > END_NUMBER_CORES or BEGIN_NUMBER_CORES < 0:
		print 'Error: number_cores are not valid.'
		return
	elif END_NUMBER_CORES > max_cores:
		print 'Error: cores exceed max_cores.'
		return

	strong_scale_test_dir = get_strong_scale_test_dir(test_dir)

	# Vary the number of jobs
	for number_jobs in xrange(BEGIN_NUMBER_JOBS, END_NUMBER_JOBS):

		print 'Running strong scale with %d jobs' % pow(2, number_jobs)
		
		# Vary the number of cores
		for number_cores in xrange(BEGIN_NUMBER_CORES, END_NUMBER_CORES):

			print 'Running strong scale with %d cores' % pow(2,  number_cores)

			single_run_parameters = dict()

			single_run_parameters['number_cores'] 	= number_cores
			single_run_parameters['number_jobs']	= number_jobs

			# Start timer
			begin_time = time.time()

			###############################################################

			run_dir = single_run(strong_scale_test_dir,
				message_server,
				single_run_parameters,
				max_cores)

			###############################################################

			# End timer
			end_time = time.time()
			run_time = end_time - begin_time

			csv_file = '%s/data_.csv' % run_dir
			data = [pow(2, number_jobs), pow(2, number_cores), run_time]

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
		print 'Single run test directory already exists'
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
		print 'Weak scale test directory already exists'
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
		print 'Strong scale test directory already exists'
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

