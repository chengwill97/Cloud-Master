import time

############################################################
#
#   SimulationNode conducts the simulations with different parameters
#
class SimulationNode:

    def __init__(self, sleepparam=1, analysisnode=None):
        self.sleeptime = sleepparam
        self.next = analysisnode

    def simulate(self):
        time.sleep(self.sleeptime)
        print "\tSimulation slept for {} second(s)\n".format(self.sleeptime)
        return "Simulation Run Successfully"

############################################################
#
#   AnalysisNode does the analysis of the data
#
class AnalysisNode:

    def __init__(self, sleepparam=1, convergenode=None):
        self.sleeptime = sleepparam
        self.next = convergenode

    def analyze(self):
        time.sleep(self.sleeptime)
        print "\t\tAnalysis slept for {} second(s)\n".format(self.sleeptime)
        return "Analysis Run Successfully"

############################################################
#
#   SimulationNode conducts the simulations
#
class ConvergenceNode:

    def __init__(self, sleepparam=1):
        self.sleeptime = sleepparam

    def converge(self):
        time.sleep(self.sleeptime)
        print "\t\t\tConvergence slept for {} second(s)\n".format(self.sleeptime)
        return "Convergence Run Successfuly"

############################################################
