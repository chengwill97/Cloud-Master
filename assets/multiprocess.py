import multiprocessing
import time
import csv
import os
import pickle
import pika

from math import pow

from nodes import SimulationNode
from nodes import AnalysisNode
from nodes import ConvergenceNode

from dataIO import read_json
from dataIO import write_json
from dataIO import append_csv

from pipeline import Pipeline


#######################################################################
#
# 	worker Function
#
# 	Worker that emaulates the Expanded Ensemble
# 	Simulate -> Analyze -> Converge sequence
#
def worker_sleeps(ch, method, properties, body):

	# Unpickle data
	body = pickle.loads(body)

	sleep_time, run_dir = body
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
		print '***********************************'
		pool.apply_async(hire_worker)

	try:
		while True:
			continue
	except KeyboardInterrupt:
		print ' [x] Exiting'
		pool.terminate()
		pool.join()


#######################################################################
#
# 	hire_worker Function
#
# 	hire process that executes tasks
#
def hire_worker():

	process_name = multiprocessing.current_process().name

	# Establish connection with the RabbitMQ server
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

	channel = connection.channel()

	channel.queue_declare(queue='sleep_queue', durable=True)

	# Sets maximum number of pre-assigned tasks to 1
	channel.basic_qos(prefetch_count=1)

	channel.basic_consume(worker_sleeps,
						  queue='sleep_queue')

	print(' [x] %s Waiting for messages. To exit press CTRL+C' % process_name)

	try:
		channel.start_consuming()
	except KeyboardInterrupt:
		pass


#######################################################################
#
# 	single_run Function
#
# 	Does a single test with the current parameteres
#
def single_run(test_dir, single_run_paramteres, max_cores):

	NUMBER_CORES 	= single_run_paramteres['number_cores']
	NUMBER_JOBS		= single_run_paramteres['number_jobs']

	print 'Starting Single Run with %d cores' % NUMBER_CORES

	# Check that parameters are valid
	if NUMBER_CORES > max_cores or NUMBER_CORES < 0:
		print 'Error: Number of cores is wrong'
		return

	run_dir = get_single_run_dir(test_dir)
	print run_dir

	workers = int(pow(2, NUMBER_CORES))

	# Map tasks to processes
	pool = multiprocessing.Pool(processes=workers)

	# Injects each tasks into a function asynchronously
	for worker in xrange(workers):
		pool.apply_async(hire_worker)
		print 'Worker %d hired' % worker

	# Establish a connection with RabbitMQ server
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
	channel = connection.channel()

	# creating a 'hello' queue to which message will be delivered
	channel.queue_declare(queue='sleep_queue', durable=True)

	total_number_tasks = int(pow(2, NUMBER_JOBS))
	for i in xrange(total_number_tasks):

		sleep_time 	= 1
		task 		= (sleep_time, run_dir)
		
		# Pickle task
		pickled_task = pickle.dumps(task)

		# Upload task parameters to queue
		channel.basic_publish(exchange='',
			  routing_key='sleep_queue',
			  body=pickled_task,
			  properties=pika.BasicProperties(
			  	delivery_mode = 2, # make message persistent
			  ))

	connection.close()

	try:
	    while True:
	        continue
	except KeyboardInterrupt:
	    print ' [x] Exiting...'
	    pool.terminate()
	    pool.join()


	###############################################################


#######################################################################
#
# 	weak_scale_run Function
#
# 	Does a weak scale test with the current weak scale parameters
#
def weak_scale_run(test_dir, weak_scale_parameters, max_cores):

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

			# Start timer
			begin_time = time.time()

			# Find available dir name for the current run with jobs_per_core
			run_dir_num = 1
			run_dir 	= '%s/run_%03d' % (weak_scale_test_dir, run_dir_num)
			while (os.path.isdir(run_dir)):
				run_dir_num += 1
				run_dir 	= '%s/run_%03d' % (weak_scale_test_dir, run_dir_num)

			# Create available dir for the current run with jobs_per_core
			os.mkdir(run_dir)

			###############################################################

			# Establish a connection with RabbitMQ server
			connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
			channel = connection.channel()

			# creating a 'hello' queue to which message will be delivered
			channel.queue_declare(queue='sleep_queue', durable=True)

			total_number_tasks = int(pow(2, jobs_per_core + number_cores))
			for i in xrange(total_number_tasks):

				sleep_time 	= 0.1
				task 		= (sleep_time, run_dir)
				
				# Pickle task
				pickled_task = pickle.dumps(task)

				# Upload task parameters to queue
				channel.basic_publish(exchange='',
					  routing_key='sleep_queue',
					  body=stream,
					  properties=pika.BasicProperties(
					  	delivery_mode = 2, # make message persistent
					  ))

			connection.close()

			###############################################################

			# assigns the tasks to cores
			assign_tasks(number_cores, worker_sleeps)

			# End timer
			end_time = time.time()
			run_time = end_time - begin_time

			csv_file = run_dir + '/data.csv'

			# Export data into csv_file
			data = [pow(2, number_cores), pow(2, jobs_per_core), run_time]
			append_csv(csv_file, data)


#######################################################################
#
# 	strong_scale_run Function
#
# 	Does a strong scale test with the current strong scale parameters
#
def strong_scale_run(test_dir, strong_scale_parameters, max_cores):

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

			# Start timer
			begin_time = time.time()

			# Find available dir name for the current run with number_cores
			run_dir_num = 1
			run_dir 	= '%s/run_%d' % (strong_scale_test_dir, run_dir_num)
			while (os.path.isdir(run_dir)):
				run_dir_num += 1
				run_dir 	= '%s/run_%d' % (strong_scale_test_dir, run_dir_num)

			# Create available dir for the current run with number_cores
			os.mkdir(run_dir)

			###############################################################

			# Create tasks
			tasks = list()
			total_number_tasks = int(pow(2, number_cores + number_jobs))
			for pipe_num in xrange(1, total_number_tasks+1):

				sleep_time 	= 1
				pipe_folder = run_dir + '/pipe_%d' % (pipe_num)
				task 		= (sleep_time, pipe_folder)

				tasks.append(task)

			###############################################################
			
			# assigns the tasks to cores
			assign_tasks(number_cores, worker_sleeps)

			# End timer
			end_time = time.time()
			run_time = end_time - begin_time

			csv_file = run_dir + '/data_%d.csv' % (run_dir_num)

			# Export data into csv_file
			data = [pow(2, number_jobs), pow(2, number_cores), run_time]
			append_csv(csv_file, data)


#######################################################################
#
# 	get_singel_run_dir Function
#
# 	Returns the output for the single run
#
def get_single_run_dir(test_dir):

	single_run_dir = test_dir + '/single_run_output'

	# Check if single_run_dir exists
	try:
		os.mkdir(single_run_dir)
	except (OSError, IOError) as e:
		print 'Weak scale test directory already exists'

	return single_run_dir


#######################################################################
#
# 	get_weak_scale_test_dir Function
#
# 	Returns the output for the weak scale test
#
def get_weak_scale_test_dir(test_dir):

	weak_scale_test_dir = test_dir + '/weak_scale_output'

	# Check if weak_scale_test_dir exists
	try:
		os.mkdir(weak_scale_test_dir)
	except (OSError, IOError) as e:
		print 'Weak scale test directory already exists'

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

	return strong_scale_test_dir