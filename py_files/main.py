"""==========================================================================================

    Title:       Bureau of Labor Statistics API Pipeline
    File:        main.py
    Author:      Dan Sagher
    Date:        3/11/25
    Description:
        This pipeline extracts, transforms, and loads data from the Bureau of Labor Statistics
        (BLS) API into a PostgreSQL database. The user uploads a CSV dimension file containing
        series names and IDs.

    Dependencies:

        External:
        - pandas
        - datetime
        - os

        Internal:
        - api_bls

    Special Notes:
        - CSV output will be moved to load() function.
        - Logging will be moved to main.py
        - Logging will be configured to include stdout
        - Additional Error handling will be moved to main
        - argparse helper flags will be added to main

=========================================================================================="""

from api_bls import BlsApiCall
import datetime as dt
import os
import argparse as ap
import subprocess
from typing import Type
import csv

def arg_parser():

    parser = ap.ArgumentParser(
        prog = 'Bureau of Labor Statistics API Pipeline',
        description = 'This program uses a list of seriesIDs, start year, and end year as input \
                        to the Bureau of Labor Statistics API. The results are parsed, cleaned, and \
                        uploaded into a local PostgreSQL database.')
    group = parser.add_mutually_exclusive_group()

    parser.add_argument('--csv-path', type=str, help="Path to CSV file containing seriesIDs", default = False)
    parser.add_argument('--series-type', type=int,choices=[1,2], help="Enter 1 for National Series or 2 for State Series", default = False)
    parser.add_argument('--start-year', type=int, help="Enter start year for query.", default = False)
    parser.add_argument('--end-year', type=int, help="Enter start year for query.", default = False)
    parser.add_argument('-n-','--number-of-series', type=int, help="Enter number of seriesIDs to input", default = None)
    parser.add_argument('-p','--ping', type=int, help="Enter number of pings to send to the BLS API.",)
    parser.add_argument('-t', '--traceroute', help="Check routing to the BLS API", action='store_true',)
    parser.add_argument('-o','--output', help="Flag to generate CSV output of results", action='store_true',)
    group.add_argument('-v','--verbose', help="Display console output during runtime.", action='store_true',)
    group.add_argument('-s','--silence', help="Display nothing output during runtime.", action='store_true',)

    return parser.parse_args()
def interactive_user_input() -> dict:

    print("=============Interactive Input====================")
    print("==================================================", '\n')

    path = input("Enter CSV path: ")
    series_type = int(input("Enter 1 for National Series, 2 for State Series: "))
    start_year = int(input('Enter start year: '))
    end_year = int(input('Enter end year: '))
    number_of_series = input('Enter desired number of series from input (Press enter for all): ')

    if series_type not in [1,2]:
        raise ValueError('Series type must be 1 for National Series or 2 for State Series.')
    if number_of_series <= 0:
        raise ValueError('Please enter a valid number of series to process.')
    
    output = {'path':path, 
              'start_year':start_year,
              'end_year':end_year,
              'series_type':series_type,
              'number_of_series':number_of_series}
    return output

def validate_path(csv_path: str) -> bool:
    pass
def validate_years(start_year: int, end_year: int) -> bool:
    pass
def read_file(path):
    pass
def setup_logging():
    pass
def ping_traceroute():
    # ping_response = subprocess.run('ping', '-c', args.ping, "https://api.bls.gov/publicAPI/v2/timeseries/data/", capture_output=True)
    # trace_response = subprocess.run('traceroute', "https://api.bls.gov/publicAPI/v2/timeseries/data/", capture_output=True)
    pass
def call_etl(api_engine: Type[BlsApiCall]) -> None:
    api_engine.extract()
    api_engine.transform()
    api_engine.load()

def main() -> None:
    """
    Handles user input and calls ETL functions from BlsApiCall class.
    """
    args = arg_parser()
    if args.csv_path and args.series_type and args.start_year and args.end_year:
        path_valid: bool = validate_path(args.csv_path)
        years_valid: bool = validate_years(args.start_year, args.end_year)

        if path_valid and years_valid and args.series_type == 1:
            file = read_file(args.csv_path)
            api_engine: Type[BlsApiCall] = BlsApiCall(args.start_year, args.end_year, national_series=file, number_of_series=args.number_of_series)
            call_etl(api_engine)

        elif path_valid and years_valid and args.series_type == 2:
            file = read_file(args.csv_path)
            api_engine: Type[BlsApiCall] = BlsApiCall(args.start_year, args.end_year,  state_series=file,  number_of_series=args.number_of_series)
            call_etl(api_engine)

    elif (args.start_year or args.end_year or args.series_type or args.csv_path) and not \
            (args.start_year and args.end_year and args.series_type and args.csv_path):
        raise ValueError("Please Specify --csv-path, --type, --start-year, and --end-year or nothing for interactive input.")
    
    else:
        output = interactive_user_input()
        path_valid: bool = validate_path(output['path'])
        years_valid: bool = validate_years(output['start_year'], output['end_year'])
    
        if path_valid and years_valid and output['series_type'] == 1:
            file = read_file(output['path'])
            api_engine: Type[BlsApiCall] = BlsApiCall(output['start_year'], output['end_year'], national_series=file, number_of_series=output['number_of_series'])
        elif path_valid and years_valid and output['series_type'] == 2:
            file = read_file(output['path'])
            api_engine: Type[BlsApiCall] = BlsApiCall(output['start_year'], output['end_year'], state_series=file, number_of_series=output['number_of_series'])
            call_etl(api_engine)

if __name__ == "__main__":
    main()
