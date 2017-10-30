from nodes import SimulationNode
from nodes import AnalysisNode
from nodes import ConvergenceNode
from pipeline import Pipeline

import multiprocessing
import time
import csv
import os

def runpipe(t):
    pipe = Pipeline(simulation=SimulationNode(sleepparam=t), analysis=AnalysisNode(sleepparam=t), convergence=ConvergenceNode(sleepparam=t))

    # find/create appropriate directory to run pipeline in
    outputfolder = "/Users/willcheng2/Documents/GitHub/Minions/minionsFiles/output/test2/pipe"
    startdir = 1
    while (os.path.isdir(outputfolder+str(startdir))):
        startdir += 1
    os.mkdir(outputfolder+str(startdir))
    pipe.run(outputfolder+str(startdir)+"/")

CSVFILE = "/Users/willcheng2/Documents/GitHub/Minions/minionsFiles/output/test2/"
CSVFILE = CSVFILE + "test2.csv"

if __name__ == "__main__":

    NUMSTEPS     = 1        # number of iterations
    NUMWORKERS   = 1        # up to 2^(NUMWORKERS) of cores to use, MAX 2^1 for MBP'13
    NUMTASKS     = 4        # up to 2^(NUMSLEEP) of tasks to use

    #run test
    for STEP in range(1,(NUMSTEPS+1)):
        print "starting step {}\n".format(STEP)

        ################################################################################################

        # Run strong and weak scaling tests in double loop
        for NUMWORKER in range(0,(NUMWORKERS+1)):
            print "starting NUMWORKER {}\n".format(NUMWORKER)
            for NUMTASK in range(0,NUMTASKS+1):
        
                print "starting NUMTASK {}\n".format(NUMTASK)
        
                datatable = []
                starttime = time.time()
                 
                # the list of tasks that multiprocessing.Pool maps the workers to
                # 100 seconds for each task
                sleeptasks = [100 for x in range(2**NUMTASK)]

                # multiprocessing maps the each NUMWORKER to each task in sleeptasks
                pool = multiprocessing.Pool(processes=2**NUMWORKER)
                results = pool.map_async(runpipe, sleeptasks)
                results.wait()
        
                endtime = time.time()
                tasktime = endtime - starttime
                datatable.append([2**NUMWORKER, 2**NUMTASK, tasktime])
                
                ##put data into CSV
                with open(CSVFILE, 'a') as database:
                    writer = csv.writer(database)
                    for data in datatable:
                        writer.writerow(data)

        ################################################################################################
