#! /home/willc97/bin/python

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

    # Server parameters
    message_server = parameters['message_server']

    # Acquire machine and test parameters

    # output_dir            : directory for the output
    output_dir              = parameters['output']
    # single_test_parameters: single test
    single_test_parameters  = parameters['single_test']
    # weak_scale_test       : testing weak scaling
    weak_scale_parameters   = parameters['weak_scale_test']
    # strong_scale_test     : testing strong scaling
    strong_scale_parameters = parameters['strong_scale_test']
    # max_cores             : max cores of the machine
    max_cores               = parameters['max_cores']

    # Check that output_dir exists
    if not os.path.isdir(output_dir):
        print ' [x] output directory does not exist for machine %s' % machine_name
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

    os.environ['CLOUDAMQP_URL'] = 'amqp://tcmpsklz:apUNGCdHdsOJ8PI3xTKnVSX6n4g-Ax9t@salamander.rmq.cloudamqp.com/tcmpsklz'

    # Run single test
    if single_test_parameters['run_test']:
        single_run(test_dir, message_server, single_test_parameters, max_cores)
    else:
        # Run Weak Scale Tests
        if weak_scale_parameters['run_test']:
            weak_scale_run(test_dir, message_server, weak_scale_parameters, max_cores)

        # Run Strong Scale Tests
        if strong_scale_parameters['run_test']:
            strong_scale_run(test_dir, message_server, strong_scale_parameters, max_cores)

    print ' [x] %s finished...' % (test_dir)
    print ' [x] Exiting gracefully'


if __name__ == '__main__':
    main()
