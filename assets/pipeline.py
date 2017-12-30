from nodes import *
from dataIO import DataIO
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

    def __init__(self, simulation=SimulationNode(), analysis=AnalysisNode(), convergence=ConvergenceNode()):
        self.simulation     = simulation
        self.analysis       = analysis
        self.convergence    = convergence

        self.outputDest = None
        self.data       = {}


    def run(self, folder):
        simulation  = self.simulation.simulate()
        analysis    = self.analysis.analyze()
        convergence = self.convergence.converge()

        self.data["Date"]              = unicode(datetime.datetime.now())
        self.data["SimulationResult"]  = simulation
        self.data["AnalysisResult"]    = analysis
        self.data["ConvergenceResult"] = convergence

        dataSend = DataIO()
        dataSend.writeData(folder, self.data)
