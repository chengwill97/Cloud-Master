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

	# Send confirmation that message was processed
	ch.basic_ack(delivery_tag=method.delivery_tag)

	# Decrease count of count_queue
	pop_remaining(ch, count_queue)

	return

