from dataIO import DataIO
import os

test = os.path.realpath('..') + '/data/input/parameters.json'

data = DataIO()

dirs = os.getcwd() + '/directories.json'

print dirs

print DataIO().readData(test)