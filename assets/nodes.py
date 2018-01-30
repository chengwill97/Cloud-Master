import time


############################################################
#
#   SimulationNode conducts the simulations with different parameters
#
class SimulationNode:

    def __init__(self, sleep_time=1, analysis_node=None, process_name=None):
        self.sleep_time     = sleep_time
        self.next           = analysis_node
        self.process_name   = process_name

    def simulate(self):
        time.sleep(self.sleep_time)
        # print '\t' + '%s: simulating for %d second(s)' % (self.process_name, self.sleep_time)
        return '%s simulated successfully' % self.process_name


############################################################
#
#   AnalysisNode does the analysis of the data
#
class AnalysisNode:

    def __init__(self, sleep_time=1, converge_node=None, process_name=None):
        self.sleep_time     = sleep_time
        self.next           = converge_node
        self.process_name   = process_name

    def analyze(self):
        time.sleep(self.sleep_time)
        # print 2*'\t' +  '%s: analyzing for %d second(s)' % (self.process_name, self.sleep_time)
        return '%s analyzed successfully' % self.process_name


############################################################
#
#   ConvergenceNode checks that the results are converging
#
class ConvergenceNode:

    def __init__(self, sleep_time=1, simulate_node=None, process_name=None):
        self.sleep_time     = sleep_time
        self.process_name   = process_name

    def converge(self):
        time.sleep(self.sleep_time)
        # print 3*'\t' + '\t%s: converging for %d second(s)' % (self.process_name, self.sleep_time)
        return '%s converged successfully' % self.process_name

