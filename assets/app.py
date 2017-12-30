from nodes import SimulationNode
from nodes import AnalysisNode
from nodes import ConvergenceNode
from pipeline import Pipeline
from dataIO import DataIO

import multiprocessing
import time
import csv
import os

def worker(arg):
    t, pipefolder = arg

    Cnode   = ConvergenceNode(sleepparam=t)
    Anode   = AnalysisNode(sleepparam=t)
    Snode   = SimulationNode(sleepparam=t)
    pipe    = Pipeline(simulation=Snode, analysis=Anode, convergence=Cnode)
    
    os.mkdir(pipefolder)
    pipe.run(pipefolder + "/")


def main():

    cwd         = os.getcwd()
    fileJSON    = DataIO()
    directories = fileJSON.readData(cwd + "/directories.json")
    output      = directories["output"]["MBP15"]

    if (output == ""):
        print "empty folder in directories.json"
        exit(1)

    testnum         = 1
    outputfolder    = "{0}/test{1}".format(output, testnum) 

    NUMWORKERS   = 2        # up to 2^(NUMWORKERS) of cores to use
    NUMTASKS     = 8        # up to 2^(NUMSLEEP) of tasks to use

    # Run strong and weak scaling tests in double loop
    for NUMWORKER in range(NUMWORKERS,NUMWORKERS+1):
        print "starting NUMWORKER 2^{0}".format(NUMWORKER)

        for NUMTASK in range(NUMWORKERS,NUMTASKS+1):
            print "starting NUMTASK 2^{0}".format(NUMTASK)

            ################################################################################################
            starttime = time.time()

            dirnum = 1
            while (os.path.isdir(outputfolder)):
                testnum += 1
                outputfolder = "{0}/test{1}".format(output, testnum)
            if (not os.path.isdir(outputfolder)):
                os.mkdir(outputfolder)

            CSVFILE = outputfolder + "/data{0}.csv".format(testnum)

            sleeptasks = []
            for i in range(2**NUMTASK):
                while (os.path.isdir(outputfolder+"/pipe{}".format(dirnum))):
                    dirnum += 1
                sleeptasks.append((1, outputfolder+"/pipe{}".format(dirnum)))
                dirnum += 1

            # multiprocessing maps the each NUMWORKER to each task in sleeptasks
            pool    = multiprocessing.Pool(processes=2**NUMWORKER)
            results = pool.map_async(worker, sleeptasks)
            results.wait()
    
            endtime     = time.time()
            tasktime    = endtime - starttime
            ################################################################################################

            ##put data into CSV
            with open(CSVFILE, 'a') as datafile:
                writer = csv.writer(datafile)
                writer.writerow([2 ** NUMWORKER, 2 ** NUMTASK, tasktime])


if __name__ == "__main__":
    main()
