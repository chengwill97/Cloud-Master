import multiprocessing
import time
import csv
import os

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
# 	Worker that emaulates the 
# 	Simulate -> Analyze -> Converge sequence
#
def worker(task):

	sleep_time, pipe_folder = task

	simulation_node 	= SimulationNode(sleepparam=sleep_time)
	analysis_node   	= AnalysisNode(sleepparam=sleep_time)
	convergence_node	= ConvergenceNode(sleepparam=sleep_time)
		
	try:
		
		os.mkdir(pipe_folder)
		
		pipeline = Pipeline(simulation=simulation_node, analysis=analysis_node, convergence=convergence_node)
		
		pipeline.run(pipe_folder + '/')

	except (OSError, IOError) as e:
		print e


#######################################################################
#
# 	get_weak_scale_test_dir Function
#
# 	Returns the output for the weak scale test
#
def get_weak_scale_test_dir(test_dir):

	weak_scale_test_dir = test_dir + '/weak_scale_output'

	# Check if weak_scale_test_dir exists
	# if false, create such directory in parent test_dir
	if not os.path.isdir(weak_scale_test_dir):
		os.mkdir(weak_scale_test_dir)

	return weak_scale_test_dir


#######################################################################
#
# 	weak_scale_run Function
#
# 	Does a weak scale test with the current weak scale paramters
#
def weak_scale_run(test_dir, weak_scale_parameters, max_cores):

	print 'Starting Weak Scale Run:\n'

	BEGIN_NUMBER_CORES 	= weak_scale_parameters['begin_number_cores']
	END_NUMBER_CORES 	= weak_scale_parameters['end_number_cores']
	BEGIN_JOBS_PER_CORE = weak_scale_parameters['begin_jobs_per_core']
	END_JOBS_PER_CORE 	= weak_scale_parameters['end_jobs_per_core']

	# Check that parameters are valid
	if BEGIN_NUMBER_CORES == END_NUMBER_CORES:
		END_NUMBER_CORES += 1
	if BEGIN_JOBS_PER_CORE == END_JOBS_PER_CORE:
		END_JOBS_PER_CORE += 1

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
	for number_cores in range(BEGIN_NUMBER_CORES, END_NUMBER_CORES):

		print 'Running weak scale with %d cores' % pow(2, number_cores)
		
		# Vary the number of jobs per core
		for jobs_per_core in range(BEGIN_JOBS_PER_CORE, END_JOBS_PER_CORE):

			print '\tRunning weak scale with %d jobs per core' % pow(2,  jobs_per_core)

			# Start timer
			begin_time = time.time()

			# Find available dir name for the current run with jobs_per_core
			run_dir_num = 1
			run_dir 	= '%s/run_%d' % (weak_scale_test_dir, run_dir_num)
			while (os.path.isdir(run_dir)):
				run_dir_num += 1
				run_dir 	= '%s/run_%d' % (weak_scale_test_dir, run_dir_num)

			# Create available dir for the current run with jobs_per_core
			os.mkdir(run_dir)

			###############################################################

			# Create tasks
			tasks = list()
			total_number_tasks = int(pow(2, jobs_per_core + number_cores))
			for pipe_num in range(1, total_number_tasks+1):

				sleep_time 	= 1
				pipe_folder = run_dir + '/pipe_%d' % (pipe_num)
				task 		= (sleep_time, pipe_folder)

				tasks.append(task)

			###############################################################

			# Map tasks to processes
			pool = multiprocessing.Pool(processes=int(pow(2,number_cores)))

			# Injects each tasks into a function asynchronously
			results = pool.map_async(worker, tasks)

			# Waits for all processes to complete before continuing
			results.wait()

			# End timer
			end_time = time.time()
			run_time = end_time - begin_time

			csv_file = run_dir + '/data_%d.csv' % (run_dir_num)

			# Export data into csv_file
			data = [pow(2, number_cores), pow(2, jobs_per_core), run_time]
			append_csv(csv_file, data)


#######################################################################
#
# 	get_strong_scale_test_dir Function
#
# 	Returns the output for the strong scale test
#
def get_strong_scale_test_dir(test_dir):

	strong_scale_test_dir = test_dir + '/strong_scale_output'

	# Check if strong_scale_test_dir exists
	# if false, create such directory in parent test_dir
	if not os.path.isdir(strong_scale_test_dir):
		os.mkdir(strong_scale_test_dir)

	return strong_scale_test_dir


#######################################################################
#
# 	strong_scale_run Function
#
# 	Does a strong scale test with the current strong scale paramters
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
	for number_jobs in range(BEGIN_NUMBER_JOBS, END_NUMBER_JOBS):

		print 'Running strong scale with %d jobs' % pow(2, number_jobs)
		
		# Vary the number of cores
		for number_cores in range(BEGIN_NUMBER_CORES, END_NUMBER_CORES):

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
			for pipe_num in range(1, total_number_tasks+1):

				sleep_time 	= 1
				pipe_folder = run_dir + '/pipe_%d' % (pipe_num)
				task 		= (sleep_time, pipe_folder)

				tasks.append(task)

			###############################################################

			# Map tasks to processes
			pool = multiprocessing.Pool(processes=int(pow(2,number_jobs)))

			# Injects each tasks into a function asynchronously
			results = pool.map_async(worker, tasks)

			# Waits for all processes to complete before continuing
			results.wait()

			# End timer
			end_time = time.time()
			run_time = end_time - begin_time

			csv_file = run_dir + '/data_%d.csv' % (run_dir_num)

			# Export data into csv_file
			data = [pow(2, number_jobs), pow(2, number_cores), run_time]
			append_csv(csv_file, data)
