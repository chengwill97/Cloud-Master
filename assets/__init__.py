import dataIO
import os

cwd = os.getcwd()
fileJSON = dataIO.DataIO()
directories = fileJSON.readData(cwd + "/directories.json")
output = directories["output"]["MBP15"]

print "{0}/test{1}".format(output, 1)