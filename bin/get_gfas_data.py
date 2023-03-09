#!/usr/bin/env python3
"""=============================================================================
Download GFAS biomass burning data for a single month.

Download a month's worth of GFAS biomass burning data from ECMWF CDS. All data
fields are downloaded into a single NetCDF file named GFAS_RAW_YYYY_MM.nc. This
script is intended to be the first part of a Download => Preprocess => Transfer
pipeline from ECMWF CDS to GCST.
============================================================================="""
import argparse
import calendar
import datetime
import logging
import os

import cdsapi


def date_string(string: str) -> datetime.date:
    """
    Verify that a date string is in the correct format for selecting a year &
    month: YYYY-MM. Used by argparse to validate a command line argument.

    Args:
        string: A string containing a year and month, in the format YYYY-MM.
    Returns:
        A datetime.date object, representative of the passed year & month.

    Raises:
        argparse.ArgumentTypeError: The passed string couldn't be converted to a
                                    datetime.date, likely due to it being
                                    incorrectly formatted
    """
    try:
        return datetime.datetime.strptime(string, "%Y-%m").date()
    except ValueError as _exception:
        _error_message = (
            f"The passed date {string} is not valid - expected format is "
            "YYYY-MM"
        )
        raise argparse.ArgumentTypeError(_error_message) from _exception


def directory_path(path_string: str) -> str:
    """
    Verify that a path string points to a valid and accessible directory Used
    by argparse to validate a command line argument.

    Args:
        path_string: A string containing the path of a possible directory.

    Returns:
        The same string that was passed to the function, if it can be verified
        as an existing and accessible directory.

    Raises:
        argparse.ArgumentTypeError: The passed string doesn't point to a valid
                                    and accessible directory.
    """
    if os.path.isdir(path_string):
        return path_string

    _error_message = (
        f"The passed output directory path {path_string} is not a path to "
        "an existing or accessible directory"
    )
    raise argparse.ArgumentTypeError(_error_message)

# TODO: Add description and example usage
def parse_command_line() -> argparse.Namespace:
    """
    Parse command line arguments and options

    Returns:
        argparse.Namespace containing command line arguments and options, on
        successful parsing.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "month",
        type=date_string,
        metavar="month {YYYY-MM}",
        help="The month of GFAS data to retrieve",
    )

    parser.add_argument(
        "-o",
        "--output-directory",
        metavar="output_directory_path",
        default="./",
        nargs=1,
        type=directory_path,
        help="Directory in which to store downloaded data files",
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    COMMAND_LINE: argparse.Namespace = parse_command_line()

    START_DATE: datetime.date = COMMAND_LINE.month
    END_DATE: datetime.date = START_DATE + datetime.timedelta(
        days=calendar.monthrange(START_DATE.year, START_DATE.month)[1] - 1
    )

    CDS_DATE_STRING: str = f"{START_DATE}/{END_DATE}"
    CDS_DATA_FIELDS: list[str] = [
        "altitude_of_plume_bottom",
        "altitude_of_plume_top",
        "injection_height",
        "mean_altitude_of_maximum_injection",
        "wildfire_combustion_rate",
        "wildfire_flux_of_acetaldehyde",
        "wildfire_flux_of_acetone",
        "wildfire_flux_of_ammonia",
        "wildfire_flux_of_benzene",
        "wildfire_flux_of_black_carbon",
        "wildfire_flux_of_butanes",
        "wildfire_flux_of_butenes",
        "wildfire_flux_of_carbon_dioxide",
        "wildfire_flux_of_carbon_monoxide",
        "wildfire_flux_of_dimethyl_sulfide",
        "wildfire_flux_of_ethane",
        "wildfire_flux_of_ethanol",
        "wildfire_flux_of_ethene",
        "wildfire_flux_of_formaldehyde",
        "wildfire_flux_of_heptane",
        "wildfire_flux_of_hexanes",
        "wildfire_flux_of_hexene",
        "wildfire_flux_of_higher_alkanes",
        "wildfire_flux_of_higher_alkenes",
        "wildfire_flux_of_hydrogen",
        "wildfire_flux_of_isoprene",
        "wildfire_flux_of_methane",
        "wildfire_flux_of_methanol",
        "wildfire_flux_of_nitrogen_oxides",
        "wildfire_flux_of_nitrous_oxide",
        "wildfire_flux_of_non_methane_hydrocarbons",
        "wildfire_flux_of_octene",
        "wildfire_flux_of_organic_carbon",
        "wildfire_flux_of_particulate_matter_d_2_5_µm",
        "wildfire_flux_of_pentanes",
        "wildfire_flux_of_pentenes",
        "wildfire_flux_of_propane",
        "wildfire_flux_of_propene",
        "wildfire_flux_of_sulphur_dioxide",
        "wildfire_flux_of_terpenes",
        "wildfire_flux_of_toluene",
        "wildfire_flux_of_toluene_lump",
        "wildfire_flux_of_total_carbon_in_aerosols",
        "wildfire_flux_of_total_particulate_matter",
        "wildfire_flux_of_xylene",
        "wildfire_fraction_of_area_observed",
        "wildfire_overall_flux_of_burnt_carbon",
        "wildfire_radiative_power",
    ]

    CDS_CLIENT: cdsapi.Client = cdsapi.Client()
    try:
        CDS_CLIENT.retrieve(
            "cams-global-fire-emissions-gfas",
            {
                "date": CDS_DATE_STRING,
                "format": "netcdf",
                "variable": CDS_DATA_FIELDS,
            },
            os.path.join(
                COMMAND_LINE.output_directory,
                f"GFAS_RAW_{START_DATE.year}_{START_DATE.month}.nc",
            ),
        )
    except Exception as exception:
        error_message: str = (
            "There was a problem retrieving data from the CDS API"
        )
        raise RuntimeError(error_message) from exception
