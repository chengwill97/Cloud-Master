import time

############################################################
#
#   SimulationNode conducts the simulations with different parameters
#
class SimulationNode:

    def __init__(self, sleep_time=1, analysisnode=None):

        self.sleep_time  = sleep_time
        self.next       = analysisnode

    def simulate(self):

        time.sleep(self.sleep_time)

        print "\tSimulation slept for %d second(s)" % self.sleep_time

        return "Simulation Run Successfully"


############################################################
#
#   AnalysisNode does the analysis of the data
#
class AnalysisNode:

    def __init__(self, sleep_time=1, convergenode=None):

        self.sleep_time  = sleep_time
        self.next       = convergenode

    def analyze(self):

        time.sleep(self.sleep_time)

        print "\t\tAnalysis slept for %d second(s)" % self.sleep_time

        return "Analysis Run Successfully"


############################################################
#
#   ConvergenceNode checks that the results are converging
#
class ConvergenceNode:

    def __init__(self, sleep_time=1):
        self.sleep_time = sleep_time

    def converge(self):

        time.sleep(self.sleep_time)

        print "\t\t\tConvergence slept for %d second(s)" % self.sleep_time
        
        return "Convergence Run Successfuly"

