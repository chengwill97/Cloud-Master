from nodes import SimulationNode
from nodes import AnalysisNode
from nodes import ConvergenceNode

from dataIO import write_json

import datetime


############################################################
#
#   Pipeline contains the three different nodes:
#       1. Simulation
#       2. Analysis
#       3. Convergence
#
#   It outputs the results and time of each node's actions in a .JSON file
#
class Pipeline:

    def __init__(self, 
        simulation=SimulationNode(), analysis=AnalysisNode(), convergence=ConvergenceNode()):
        self.simulation     = simulation
        self.analysis       = analysis
        self.convergence    = convergence 
        self.results        = dict()

    # Execute nodes in EE algorithm
    def run(self):
        self.results['date']                = unicode(datetime.datetime.now())
        self.results['simulation_result']   = self.simulation.simulate()
        self.results['analysis_result']     = self.analysis.analyze()
        self.results['convergence_result']  = self.convergence.converge()
        return self.results
