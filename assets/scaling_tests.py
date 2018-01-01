import multiprocessing
import time
import csv
import os

from nodes      import SimulationNode
from nodes      import AnalysisNode
from nodes      import ConvergenceNode

from pipeline   import Pipeline
from dataIO     import DataIO

from math import pow

def worker(task):

    t, pipefolder = task

    Cnode   = ConvergenceNode(sleepparam=t)
    Anode   = AnalysisNode(sleepparam=t)
    Snode   = SimulationNode(sleepparam=t)
    pipe    = Pipeline(simulation=Snode, analysis=Anode, convergence=Cnode)
    
    os.mkdir(pipefolder)
    pipe.run(pipefolder + "/")


def get_weak_scale_test_dir(test_dir):

	weak_scale_test_dir = test_dir + '/weak_scale_output'

	# Check if weak_scale_test_dir exists
	# if false, create such directory in parent test_dir
	if not os.path.isdir(weak_scale_test_dir):
		os.mkdir(weak_scale_test_dir)

	return weak_scale_test_dir


def weak_scale_run(test_dir, weak_scale_parameters, max_cores):

	print 'Starting Weak Scale Run:\n'

	LOWER_BOUND_CORES 	= weak_scale_parameters['lower_bound_cores']
	UPPER_BOUND_CORES 	= weak_scale_parameters['upper_bound_cores']
	START_JOBS_PER_CORE = weak_scale_parameters['start_jobs_per_core']
	END_JOBS_PER_CORE 	= weak_scale_parameters['end_jobs_per_core']

	# Check that parameters are valid
	if START_JOBS_PER_CORE == END_JOBS_PER_CORE:
		END_JOBS_PER_CORE += 1
	if LOWER_BOUND_CORES == UPPER_BOUND_CORES:
		UPPER_BOUND_CORES += 1

	# Check that parameters are valid
	if LOWER_BOUND_CORES > UPPER_BOUND_CORES or LOWER_BOUND_CORES < 0:
		print 'Error: bounds on cores are not valid.'
		return
	elif START_JOBS_PER_CORE > END_JOBS_PER_CORE:
		print 'Error: jobs_per_core are not valid.'
		return
	elif pow(2, UPPER_BOUND_CORES) > max_cores:
		print 'Error: ores exceed max_cores.'
		return

	weak_scale_test_dir = get_weak_scale_test_dir(test_dir)
 
 	#######################################################################
	#
	# Run weak scaling test
	#

	# Vary the number of cores
	for number_cores in range(LOWER_BOUND_CORES, UPPER_BOUND_CORES):

		print 'Running weak scale with %d cores' % pow(2, number_cores)
		
		# Vary the number of jobs per core
		for jobs_per_core in range(START_JOBS_PER_CORE, END_JOBS_PER_CORE):

			print 'Running weak scale with %d jobs per core' % pow(2,  jobs_per_core)

			# Start timer
			start_time = time.time()

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
			tasks = []
			total_number_tasks = int(pow(2, jobs_per_core + number_cores))
			for pipe in range(1, total_number_tasks):

				sleep_time 	= 1
				pipe_folder = run_dir + '/pipe_%d' % (pipe_num)
				task 		= (sleep_time, pipefolder)

				tasks.append(task)


			# Map tasks to processes
			pool = multiprocessing.Pool(processes=int(pow(2,number_cores)))

			# Injects each tasks into a function
			results = pool.map_async(worker, tasks)

			# Waits for all processes to complete before continuing
			results.wait()

			###############################################################

			# End timer
			end_time = time.time()
			run_time = end_time - start_time

			# Export data into CSV file
			csv_file = run_dir + '/data%d.csv' % (run_dir_num)
			with open(csv_file, 'a') as file:
				writer 	= csv.writer(file)
				data 	= [pow(2, number_cores), pow(2, jobs_per_core), run_time]
				writer.writerow(data)


def get_strong_scale_test_dir(test_dir):

	strong_scale_test_dir = test_dir + '/strong_scale_output'

	# Check if strong_scale_test_dir exists
	# if false, create such directory in parent test_dir
	if not os.path.isdir(strong_scale_test_dir):
		os.mkdir(strong_scale_test_dir)

	return strong_scale_test_dir




