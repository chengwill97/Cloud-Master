import multiprocessing
import time
import csv
import os
import pickle
import pika
import random
import sys

from math import pow

from communication import pop_remaining
from communication import Communication

from dataIO import read_json
from dataIO import write_json
from dataIO import append_csv

from nodes import SimulationNode
from nodes import AnalysisNode
from nodes import ConvergenceNode

from pipeline import Pipeline

from tasks import worker_sleeps


#######################################################################
#
# 	hire_worker Function
#
# 	hire process that executes tasks
#
def hire_worker(task_queue, count_queue):

	# Establish connection with the RabbitMQ server
	worker_com = Communication()

	worker_com.channel.queue_declare(queue=task_queue, durable=True)

	# Sets maximum number of pre-assigned tasks to 1
	worker_com.channel.basic_qos(prefetch_count=1)
	worker_com.channel.basic_consume(worker_sleeps,
						  queue=task_queue)
	
	process_name = multiprocessing.current_process().name
	print(' [w] %s Waiting for messages.' % process_name)
	
	try:
		worker_com.channel.start_consuming()
	except KeyboardInterrupt:
		worker_com.connection.close()


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

	print ' [s] Starting Single Run with %02d jobs and %02d cores' % (total_number_jobs, number_cores)

	# Check that parameters are valid
	if NUMBER_CORES > max_cores or NUMBER_CORES < 0:
		print ' [x] Error: Number of cores is not valid'
		return ' [x] Error: Number of cores is not valid'

	run_dir = get_single_run_dir(test_dir)

	# workers are how many cores are to be used
	workers = int(pow(2, NUMBER_CORES))

	# Map tasks to processes
	pool = multiprocessing.Pool(processes=workers)

	# Injects each tasks into a function asynchronously
	for worker in xrange(workers):
		pool.apply_async(hire_worker,[task_queue, count_queue])
		print ' [s] Worker #%02d hired' % worker
	
	'''
	Set the connection parameters to connect to 'server' on port 5672
	on the / virtual host using the username "guest" and password "guest"
	and establish a connection with RabbitMQ server
	'''
	
	manager_com = Communication()
	connection = manager_com.connection
	channel = manager_com.channel

	manager_com.queue_start(queue_name=task_queue, durable=True, purge=True)
	manager_com.queue_start(queue_name=count_queue, durable=False, purge=True)

	manager_com.print_queue_info()
	
	try:

		print ' [s] tasks completed:\n',
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

				print '%04d/%04d' % (
					task_count,
					total_number_jobs)
				
				task_count += 1

				sleep_time 	= 0
				task 		= (sleep_time, run_dir)
				
				# Pickle task to upload to task_queue
				pickled_task = pickle.dumps(
					{'data': task,
					 'count_queue' : count_queue
					})

				# Upload task parameters to queue
				manager_com.channel.basic_publish(exchange='',
					  routing_key=task_queue,
					  body=pickled_task,
					  properties=pika.BasicProperties(
						delivery_mode = 2, # make message persistent
					  ))

				# count_queue counts the number of tasks assigned and not completed
				manager_com.channel.basic_publish(exchange='',
					  routing_key=count_queue,
					  body='',
					  properties=pika.BasicProperties(
						delivery_mode = 2, # make message persistent
					  ))

				if total_number_jobs <= task_count:
					break

			# Waits until all tasks are completed
			begin_time = time.time()
			while sum(manager_com.queue_depth([task_queue, count_queue])) > 0:

				end_time = time.time()
				if end_time - begin_time >= 60:

					message_counts = manager_com.queue_depth([task_queue, count_queue])
					print ' [x] Exiting due to queue timeout'
					print ' [x] in run with %d number cores and %d total number of jobs' % (number_cores, total_number_jobs)

					manager_com.print_queue_info()

					manager_com.purge_all_queues()

					# Close connection
					manager_com.connection.close()
					pool.terminate()
					pool.join()
					
					return ' [x] Error: Queue Timeout Error'

			if total_number_jobs <= task_count:
				break

	except KeyboardInterrupt:

		print ' [x] Exiting due to KeyboardInterrupt exception'
		print ' [x] in run with %d number cores and %d total number of jobs' % (number_cores, total_number_jobs)

		# Print queue and corresponding message counts
		manager_com.print_queue_info()

		# Purge queues
		manager_com.purge_all_queues()

		manager_com.connection.close()
		pool.terminate()
		pool.join()

		return ' [x] Error: KeyboardInterrupt'

	manager_com.connection.close()
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

	print ' [ws] Starting Weak Scale Run:\n'

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

	for number_cores in xrange(BEGIN_NUMBER_CORES, END_NUMBER_CORES): 			# Vary the number of cores
		for jobs_per_core in xrange(BEGIN_JOBS_PER_CORE, END_JOBS_PER_CORE):	# Vary the number of jobs per core

			print '\n [ws] Running weak scale with %d cores and %d jobs per core' % (pow(2, number_cores), pow(2,  jobs_per_core))

			single_run_parameters = dict()

			single_run_parameters['number_cores'] 	= number_cores
			single_run_parameters['number_jobs']	= number_cores + jobs_per_core 

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

			data = [single_run_dir,
				pow(2, number_cores),
				pow(2, jobs_per_core),
				run_time]

			# Export data into csv_file
			append_csv(csv_file, data)


#######################################################################
#
# 	strong_scale_run Function
#
# 	Does a strong scale test with the current strong scale parameters
#
def strong_scale_run(test_dir, message_server, strong_scale_parameters, max_cores):

	print '[ss] Starting Strong Scale Run:\n'

	BEGIN_NUMBER_JOBS 	= strong_scale_parameters['begin_number_jobs']
	END_NUMBER_JOBS 	= strong_scale_parameters['end_number_jobs']+1
	BEGIN_NUMBER_CORES 	= strong_scale_parameters['begin_number_cores']
	END_NUMBER_CORES 	= strong_scale_parameters['end_number_cores']+1

	# Check that parameters are valid
	if BEGIN_NUMBER_CORES == END_NUMBER_CORES:
		END_NUMBER_CORES += 1
	if BEGIN_NUMBER_JOBS == END_NUMBER_JOBS:
		END_NUMBER_JOBS += 1

	# Check that parameters are valid
	if BEGIN_NUMBER_JOBS > END_NUMBER_JOBS or BEGIN_NUMBER_JOBS < 0:
		print ' [ss] Error: bounds on cores are not valid.'
		return
	elif BEGIN_NUMBER_CORES > END_NUMBER_CORES or BEGIN_NUMBER_CORES < 0:
		print ' [ss] Error: number_cores are not valid.'
		return
	elif END_NUMBER_CORES > max_cores:
		print ' [ss] Error: cores exceed max_cores.'
		return

	strong_scale_test_dir = get_strong_scale_test_dir(test_dir)

	csv_file = '%s/data.csv' % strong_scale_test_dir
	header = ['Test Directory', '2 ^ Number of Jobs', '2 ^ Number of Cores', 'Run Time']
	append_csv(csv_file, header)

	for number_jobs in xrange(BEGIN_NUMBER_JOBS, END_NUMBER_JOBS):			# Vary the number of jobs
		for number_cores in xrange(BEGIN_NUMBER_CORES, END_NUMBER_CORES):	# Vary the number of cores

			print '\n [ss] Running strong scale with %d jobsand %d cores' % (pow(2, number_jobs), pow(2,  number_cores))

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

			data = [single_run_dir,
				pow(2, number_jobs),
				pow(2, number_cores),
				run_time]

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

