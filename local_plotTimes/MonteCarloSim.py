import random
import math
import time


def montecarlo(points):

    pointsinside = 0
    for i in range(0, points):
        x = random.random()
        y = random.random()
        if math.sqrt(x**2 + y**2) <= 1:
            pointsinside += 1


    return 4.0 * pointsinside / points

start_time = time.time()
num = montecarlo(10**7)
print "The estimated value of pi is " + str(num)

print "The evaluation took {0} seconds to run".format(time.time() - start_time)
