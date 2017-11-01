from nodes import SimulationNode
from nodes import AnalysisNode
from nodes import ConvergenceNode
from pipeline import Pipeline
from dataIO import DataIO

import multiprocessing
import time
import csv
import os

cwd = os.getcwd()
fileJSON = DataIO()
directories = fileJSON.readData(cwd + "/directories.json")
output = directories["output"]["MBP15"]

testnum = 1
outputfolder = "{0}/test{1}".format(output, testnum)

"{0}/test{1}".format(output, testnum)

def worker(arg):
    t, startdir = arg

    pipe = Pipeline(simulation=SimulationNode(sleepparam=t), analysis=AnalysisNode(sleepparam=t), convergence=ConvergenceNode(sleepparam=t))
    # find/create appropriate directory to run pipeline in
    pipefolder = "/pipe{}".format(startdir)

    os.mkdir(outputfolder + pipefolder)
    pipe.run(outputfolder + pipefolder + "/")

def main():
    CSVFILE = outputfolder + "/data{}.csv".format(testnum)

    NUMWORKERS   = 2        # up to 2^(NUMWORKERS) of cores to use
    NUMTASKS     = 8        # up to 2^(NUMSLEEP) of tasks to use

    #run test


    # Run strong and weak scaling tests in double loop
    dirnum = 1
    for NUMWORKER in range(NUMWORKERS,NUMWORKERS+1):

        print "starting NUMWORKER 2^{}".format(NUMWORKER)

        for NUMTASK in range(NUMTASKS,NUMTASKS+1):
    
            print "starting NUMTASK 2^{}".format(NUMTASK)

            ################################################################################################
            starttime = time.time()

            sleeptasks = []
            for i in range(2**NUMTASK):
                while (os.path.isdir(outputfolder+"/pipe{}".format(dirnum))):
                    dirnum += 1
                sleeptasks.append((50, dirnum))
                dirnum += 1

            # multiprocessing maps the each NUMWORKER to each task in sleeptasks
            pool = multiprocessing.Pool(processes=2**NUMWORKER)
            results = pool.map_async(worker, sleeptasks)
            results.wait()
    
            endtime = time.time()
            tasktime = endtime - starttime
            ################################################################################################

            ##put data into CSV
            with open(CSVFILE, 'a') as datafile:
                writer = csv.writer(datafile)
                writer.writerow([2 ** NUMWORKER, 2 ** NUMTASK, tasktime])
                # for data in [2**NUMWORKER, 2**NUMTASK, tasktime]:
                #     writer.writerow(data)


if __name__ == "__main__":

    if (output == ""):
        print "empty folder in directories.json"
        exit(1)

    while (os.path.isdir(outputfolder)):
        testnum += 1
        outputfolder = "{0}/test{1}".format(output, testnum)

    if (not os.path.isdir(outputfolder)):
        os.mkdir(outputfolder)

    main()
