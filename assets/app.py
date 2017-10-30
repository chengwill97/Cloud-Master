from nodes import SimulationNode
from nodes import AnalysisNode
from nodes import ConvergenceNode
from pipeline import Pipeline

import multiprocessing
import time
import csv
import os

def sleepnow(t):
    pipe = Pipeline(simulation=SimulationNode(sleepparam=t), analysis=AnalysisNode(sleepparam=t), convergence=ConvergenceNode(sleepparam=t))

    # find/create appropriate directory to run pipeline in
    folder = "/Users/WillC/Documents/Rutgers/Research/RADICAL/minionsFiles/output/"

    startdir = 0
    while (os.path.isdir(folder+str(startdir))):
        startdir += 1
    os.mkdir(folder+str(startdir))
    pipe.run(folder+str(startdir)+"/")

folder = "/Users/WillC/Documents/Rutgers/Research/RADICAL/minionsFiles/"
filename = "concurrencytest.csv"
filepath = folder + filename

if __name__ == "__main__":

    NUMSTEPS     = 1        # number of iterations
    NUMWORKERS   = 2        # up to 2^(NUMWORKERS) of cores to use (up to 2^2)
    NUMTASKS     = 4        # up to 2^(NUMSLEEP) of tasks to use (up to 2^4)

    #run test
    for STEP in range(1,(NUMSTEPS+1)):
        print "starting step {}\n".format(STEP)

        ################################################################################################

        # Run strong and weak scaling tests in double loop
        for NUMWORKER in range(1,(NUMWORKERS+1)):
            print "starting NUMWORKER {}\n".format(NUMWORKER)
            for sleeptime in range(NUMTASKS+1):
        
                if (NUMWORKER == 0 and sleeptime >= 2):
                    break
        
                print "starting sleeptime {}\n".format(sleeptime)
        
                # datatable = []
                # starttime = time.time()
                #
                # the list of tasks that multiprocessing.Pool maps the workers to
                # 100 seconds for each task
                sleeptasks = [1 for x in range(2**sleeptime)]
        
                # multiprocessing maps the each NUMWORKER to each task in sleeptasks
                pool = multiprocessing.Pool(processes=2**NUMWORKER)
                results = pool.map_async(sleepnow, sleeptasks)
                results.wait()
        
                # endtime = time.time()
                # tasktime = endtime - starttime
                # datatable.append([2**NUMWORKER, 2**sleeptime, tasktime])
                #
                # put data into CSV
                # with open(filepath, 'a') as database:
                #     writer = csv.writer(database)
                #     for data in datatable:
                #         writer.writerow(data)

        ################################################################################################
