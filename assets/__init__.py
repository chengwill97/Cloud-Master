from dataIO import DataIO
import datetime
import os
from pipeline import Pipeline

datafile = DataIO()

data2 = {"hello": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
data2["world"] = "whatsup"

folder = "/Users/WillC/Documents/Rutgers/Research/RADICAL/minionsFiles/output/"

startdir = 0

while (os.path.isdir(folder + str(startdir))):
    startdir += 1
    print startdir

pipe = Pipeline()

folder = "/Users/WillC/Documents/Rutgers/Research/RADICAL/minionsFiles/output/"

startdir = 0

while (os.path.isdir(folder+str(startdir))):
    print os.path.isdir(folder + str(startdir)) + "False"
    startdir += 1

print os.path.isdir(folder + str(startdir))

pipe.run(folder+str(startdir))

print folder+str(startdir)

#datafile.writeData(folder+str(startdir), data2)
