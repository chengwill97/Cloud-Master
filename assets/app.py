import os

from dataIO import read_json
from dataIO import write_json

from scaling_tests import weak_scale_run
from scaling_tests import strong_scale_run


def main():

    # Read in the parameters.json file
    parameters_path     = os.path.realpath('..') + '/data/input/parameters.json'
    input_file          = read_json(parameters_path)

    # Acquire machine name for the correct parameters
    machine_name        = input_file['machine']
    input_parameters    = input_file[machine_name + '_parameters']

    # Acquire machine and test parameters
    # output_dir            : directory for the output
    # weak_scale_test       : testing weak scaling
    # strong_scale_test     : testing strong scaling
    # max_cores             : max cores of the machine
    output_dir              = input_parameters['output']
    weak_scale_parameters   = input_parameters['weak_scale_test']
    strong_scale_parameters = input_parameters['strong_scale_test']
    max_cores               = input_parameters['max_cores']

    # Check that output_dir exists
    if not os.path.isdir(output_dir):
        print 'output directory does not exist for machine %s' % machine_name
        exit(1)

    # Find available dir name for this test in output_dir
    test_dir_num    = 1
    test_dir        = '%s/%s_test_%d' % (output_dir, machine_name, test_dir_num)
    while (os.path.isdir(test_dir)):
        test_dir_num += 1
        test_dir = '%s/%stest_%d' % (output_dir, machine_name, test_dir_num)

    # Create available dir for this test in output_dir
    os.mkdir(test_dir)

    # Copy test parameters into test folder
    copy_parameters_file = test_dir + '/parameters_%d.json' % test_dir_num
    write_json(copy_parameters_file, input_file)

    # Run Weak Scale Tests
    if weak_scale_parameters['run_test']:
        weak_scale_run(test_dir, weak_scale_parameters, max_cores)

    # Run Strong Scale Tests
    if strong_scale_parameters['run_test']:
        strong_scale_run(test_dir, strong_scale_parameters, max_cores)


if __name__ == '__main__':
    main()
