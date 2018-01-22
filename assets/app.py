#!/usr/bin/env python2.7

import os

from dataIO import read_json
from dataIO import write_json

from multiprocess import weak_scale_run
from multiprocess import strong_scale_run
from multiprocess import single_run


def main():

    # Read in the parameters.json file
    parameters_path     = os.path.realpath('..') + '/data/input/parameters.json'
    input_file          = read_json(parameters_path)

    # Acquire machine name for the correct parameters
    machine_name        = input_file['machine']
    parameters          = input_file[machine_name + '_parameters']

    # Queue parameters
    queue = parameters['queue_name']

    # Acquire machine and test parameters
    # output_dir            : directory for the output
    # weak_scale_test       : testing weak scaling
    # strong_scale_test     : testing strong scaling
    # max_cores             : max cores of the machine
    output_dir              = parameters['output']
    single_test_parameters  = parameters['single_test']
    weak_scale_parameters   = parameters['weak_scale_test']
    strong_scale_parameters = parameters['strong_scale_test']
    max_cores               = parameters['max_cores']

    # Check that output_dir exists
    if not os.path.isdir(output_dir):
        print 'output directory does not exist for machine %s' % machine_name
        exit(1)

    # Find available dir name for this test in output_dir
    test_dir_num    = 1
    test_dir        = '%s/%s_test_%03d' % (output_dir, machine_name, test_dir_num)
    while (os.path.isdir(test_dir)):
        test_dir_num += 1
        test_dir = '%s/%s_test_%03d' % (output_dir, machine_name, test_dir_num)

    # Create available dir for this test in output_dir
    os.mkdir(test_dir)

    # Copy test parameters into test folder
    copy_parameters_file = test_dir + '/parameters_%03d.json' % test_dir_num
    write_json(copy_parameters_file, input_file)

    # Run single test
    if single_test_parameters['run_test']:
        single_run(test_dir, max_cores, queue, single_test_parameters)
    else:
        # Run Weak Scale Tests
        if weak_scale_parameters['run_test']:
            weak_scale_run(test_dir, max_cores, queue, weak_scale_parameters)

        # Run Strong Scale Tests
        if strong_scale_parameters['run_test']:
            strong_scale_run(test_dir, max_cores, queue, strong_scale_parameters)


if __name__ == '__main__':
    main()
