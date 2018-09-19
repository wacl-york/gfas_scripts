#!/usr/bin/env python
"""
Combine halved monthly CAMS Global Fire Assimilation System data.
"""
import argparse
from json import load
import sys

import netCDF4 as nc4
import numpy as np

def simple_mode(data):
    """
    Return the modal value from a collection of values.
    """
    unique, counts = np.unique(data, return_counts=True)
    return unique[np.argmax(counts)]

def get_script_args():
    """
    Get command line arguments and options.
    """
    description = 'Combine two halves of CAMS GFAS monthly data'
    arg_parser = argparse.ArgumentParser(description=description)
    arg_parser.add_argument('first_half', metavar='first_half', nargs=1, type=str,
                            help='First half of monthly data')
    arg_parser.add_argument('second_half', metavar='second_half', nargs=1, type=str,
                            help='Second half of monthly data')
    arg_parser.add_argument('output_file', metavar='output_file', nargs=1, type=str,
                            help='Output NetCDF filename')
    arg_parser.add_argument('-v', '--variable-spec', metavar='variable_spec',
                            default='variable_spec.json', nargs=1, type=str,
                            help='JSON-format variable spec file')
    return arg_parser.parse_args()


def read_variable_spec(variable_spec_filename):
    """
    Read variable codes, names, and units from a variable spec JSON file.
    """
    try:
        with open(variable_spec_filename) as variable_spec:
            return load(variable_spec)
    except OSError as exception:
        sys.stderr.write('ERROR: CAUGHT EXCEPTION WHEN READING VARIABLE SPEC')
        sys.stderr.write(f'{exception}')
        exit(1)

def process_time_dimension(output_dataset, input_dataset_1, input_dataset_2):
    """
    Preprocess input_dataset time dimension, storing result in output_dataset.
    """
    output_time = output_dataset.createVariable('time', np.int32, ('time',), zlib=True)
    output_time.units = 'hours since 1970-01-01 00:00:0.0'
    output_time.long_name = 'time'
    output_time.calendar = 'gregorian'

    time_list_1 = np.ndarray.tolist(input_dataset_1.variables['time'][:] - 613608)
    time_list_2 = np.ndarray.tolist(input_dataset_2.variables['time'][:] - 613608)

    output_time[:] = time_list_1 + time_list_2

def process_lat_dimension(output_dataset, input_dataset):
    """
    Preprocess input_dataset latitude dimension, storing result in
    output_dataset.
    """
    output_latitude = output_dataset.createVariable('lat', np.float32, ('lat',),
                                                    zlib=True)
    output_latitude.units = 'degrees_north'
    output_latitude.long_name = 'latitude'
    output_latitude[:] = np.ndarray.tolist(input_dataset.variables['latitude'][::-1])

def process_lon_dimension(output_dataset, input_dataset):
    """
    Preprocess input_dataset longitude dimension, storing result in
    output_dataset.
    """
    output_longitude = output_dataset.createVariable('lon', np.float32, ('lon',),
                                                     zlib=True)
    output_longitude.units = 'degrees_east'
    output_longitude.long_name = 'longitude'
    output_longitude[:] = np.ndarray.tolist(input_dataset.variables['longitude'][:])

def process_dimensions(output_dataset, input_dataset_1, input_dataset_2):
    """
    Preprocess time, latitude, and longitude.
    """
    process_time_dimension(output_dataset, input_dataset_1, input_dataset_2)
    process_lat_dimension(output_dataset, input_dataset_1)
    process_lon_dimension(output_dataset, input_dataset_1)

def process_variable(output_dataset, input_dataset_1, input_dataset_2,
                     metadata):
    """
    General-purpose function for preprocessing input_dataset variables.
    """
    sys.stderr.write(f"INFO: Processing {metadata['code']}\n")

    if metadata['code'] not in input_dataset_1.variables:
        sys.stderr.write((f"WARNING: Variable {metadata['code']} specified in variable"
                          "spec, but not found in input datasets."))
        return None

    miss_value = np.float32(-1.E-31)

    output_variable = output_dataset.createVariable(metadata['code'], np.float32,
                                                    ('time', 'lat', 'lon'),
                                                    fill_value=miss_value,
                                                    chunksizes=(360, 181, 1),
                                                    zlib=True)
    output_variable.units = metadata['unit']
    output_variable.long_name = metadata['name']
    output_variable.missing_value = miss_value

    input_data_1 = input_dataset_1.variables[metadata['code']][:, ::-1, :]
    input_data_2 = input_dataset_2.variables[metadata['code']][:, ::-1, :]

    if metadata['code'] == 'mami':
        input_data_1[input_data_1 == simple_mode(input_data_1)] = miss_value
        input_data_2[input_data_2 == simple_mode(input_data_2)] = miss_value
    else:
        input_data_1[input_data_1 == simple_mode(input_data_1)] = 0.0
        input_data_2[input_data_2 == simple_mode(input_data_2)] = 0.0

    output_variable[:, :, :] = np.concatenate((input_data_1, input_data_2))

def process_emission_heights(output_dataset):
    """
    Preprocess mean altitude of maximum injection variable.
    """
    miss_value = np.float32(-1.E-31)

    heights = output_dataset.variables['mami'][:, :, :]
    cofire_values = output_dataset.variables['cofire'][:, :, :]

    indices = cofire_values == 0.0
    heights[indices] = miss_value

    indices = (heights != miss_value) & (cofire_values > -1.0) & (heights < 1.0)
    heights[indices] = 0.0

    output_dataset.variables['mami'][:, :, :] = heights

def main():
    """
    Main entry point for this script.
    """
    script_args = get_script_args()

    try:
        with open(script_args.variable_spec[0]) as variable_spec_file:
            variable_spec = load(variable_spec_file)
    except OSError as exception:
        error_message = ('ERROR: Unable to open variable spec file '
                         f'{script_args.variable_spec[0]} for reading\n')
        sys.stderr.write(error_message)
        sys.stderr.write(f'{exception.strerror}\n')
        exit(1)

    try:
        input_dataset_1 = nc4.Dataset(script_args.first_half[0], mode='r')
        input_dataset_1.set_auto_mask(False)
    except OSError as exception:
        error_message = ('ERROR: Unable to open first half file '
                         f'{script_args.first_half[0]} for reading\n')
        sys.stderr.write(error_message)
        sys.stderr.write(f'{exception.strerror}\n')
        exit(1)

    try:
        input_dataset_2 = nc4.Dataset(script_args.second_half[0], mode='r')
        input_dataset_2.set_auto_mask(False)
    except OSError as exception:
        error_message = ('ERROR: Unable to open second half file '
                         f'{script_args.second_half[0]} for reading\n')
        sys.stderr.write(error_message)
        sys.stderr.write(f'{exception.strerror}\n')
        exit(1)

    try:
        output_dataset = nc4.Dataset(script_args.output_file[0], mode='w')
        output_dataset.set_auto_mask(False)
    except OSError as exception:
        error_message = ('ERROR: Unable to open output file '
                         f'{script_args.second_half[0]} for writing\n')
        sys.stderr.write(error_message)
        sys.stderr.write(f'{exception.strerror}\n')
        exit(1)

    output_dataset.createDimension('lon', 3600)
    output_dataset.createDimension('lat', 1800)
    output_dataset.createDimension('time', None)

    process_dimensions(output_dataset, input_dataset_1, input_dataset_2)

    for metadata in variable_spec['variables']:
        process_variable(output_dataset, input_dataset_1, input_dataset_2,
                         metadata)

    process_emission_heights(output_dataset)

    input_dataset_1.close()
    input_dataset_2.close()
    output_dataset.close()
#=========================================================================================
if __name__ == '__main__':
    main()
