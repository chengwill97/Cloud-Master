from dataIO import DataIO
import os

test = os.path.realpath('..') + '/data/input/parameters.json'

data = DataIO()

print "%s" % test

dirs = os.getcwd() + '/directories.json'

print dirs

print data.readData(test)

data2 = data.readData(test)

print data2['machine']

print data2[data2['machine'] + '_parameters']