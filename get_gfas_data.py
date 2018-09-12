#!/usr/bin/env python
"""
Acquire CAMS Global Fire Assimilation data for passed year, month combination,
storing in passed directory.
"""
import argparse
from calendar import monthrange
from copy import deepcopy
from datetime import date
import os
import sys

from ecmwfapi import ECMWFDataServer
from ecmwfapi.api import APIException

# PARAMETERS COMMON TO EACH DATA REQUEST
PARAM = ('80.210/81.210/82.210/83.210/84.210/85.210/86.210/87.210/88.210/89.210/90.210/'
         '91.210/92.210/97.210/99.210/100.210/102.210/103.210/104.210/105.210/106.210/'
         '107.210/108.210/109.210/110.210/111.210/112.210/113.210/114.210/115.210/'
         '117.210/118.210/119.210/120.210/231.210/232.210/233.210/234.210/235.210/'
         '236.210/237.210/238.210/239.210/240.210/241.210')
REQUEST_PARAMS = {
    "class": "mc",
    "dataset": "cams_gfas",
    "expver": "0001",
    "levtype": "sfc",
    "param": PARAM,
    "step": "0-24",
    "stream": "gfas",
    "format": "netcdf",
    "time": "00",
    "type": "ga",
}

def month_to_mars_date_strings(year, month):
    """
    Generate and return MARS date strings for passed year.
    """
    first_half_start_date = date(year, month, 1)
    first_half_end_date = date(year, month, 15)
    second_half_start_date = date(year, month, 16)
    second_half_end_date = date(year, month, monthrange(year, month)[1])

    return (f'{first_half_start_date}/to/{first_half_end_date}',
            f'{second_half_start_date}/to/{second_half_end_date}')

def retrieve_gfas_data(server, year, month, destination):
    """
    Retrieve GFAS data for the passed month in year, storing in destination
    directory.
    """
    mars_date_strings = month_to_mars_date_strings(year, month)

    for index, letter in enumerate(('a', 'b')):
        request = deepcopy(REQUEST_PARAMS)
        target_filename = os.path.join(destination,
                                       f'GFAS_RAW_{year}_{month}_{letter}.nc')
        request.update({"date": mars_date_strings[index],
                        "target": target_filename})
        try:
            server.retrieve(request)
        except APIException as api_exception:
            error_message = ('WARNING: CAUGHT ECMWF API EXCEPTION ON '
                             f'REQUEST WITH PARAMETERS {request}')
            sys.stderr.write(error_message)
            sys.stderr.write(f'WARNING: EXCEPTION MESSAGE IS {api_exception}')

def get_script_args():
    """
    Get command line arguments and options.
    """
    description = 'Download two halves of CAMS GFAS monthly data'
    arg_parser = argparse.ArgumentParser(description=description)
    arg_parser.add_argument('year', metavar='year', nargs=1, type=int,
                            choices=range(2003, 2019),
                            help='Year in which monthly data occurs')
    arg_parser.add_argument('month', metavar='month', nargs=1, type=int,
                            choices=range(1, 13),
                            help='Month of data to download, from 1-12')
    arg_parser.add_argument('-o', '--output-directory', metavar='output_directory',
                            default='./', nargs=1, type=str,
                            help='Directory in which to store downloaded data files')
    return arg_parser.parse_args()

def main():
    """
    Read command line arguments and download appropriate GFAS data files.
    """
    script_args = get_script_args()

    data_server = ECMWFDataServer()
    retrieve_gfas_data(data_server, script_args.year[0], script_args.month[0],
                       script_args.output_directory[0])
#=========================================================================================
if __name__ == '__main__':
    main()
