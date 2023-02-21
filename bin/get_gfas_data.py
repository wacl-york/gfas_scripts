#!/usr/bin/env python3
"""=============================================================================

============================================================================="""
import argparse
import calendar
import datetime
import logging
import os

import cdsapi


def date_string(date_string: str) -> datetime.date:
    """ """
    try:
        return datetime.datetime.strptime(date_string, "%Y-%m").date()
    except ValueError as exception:
        error_message = (
            f"The passed date {date_string} is not valid - expected format is "
            "YYYY-MM"
        )
        raise argparse.ArgumentTypeError(error_message) from exception


def directory_path(path_string: str) -> str:
    """ """
    if os.path.isdir(path_string):
        return path_string
    else:
        error_message = (
            f"The passed output directory path {path_string} is not a path to an "
            "existing directory"
        )
        raise argparse.ArgumentTypeError(error_message)


def parse_command_line() -> argparse.Namespace:
    """ """
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
        error_message = "There was a problem retrieving data from the CDS API"
        raise RuntimeError(error_message) from exception
