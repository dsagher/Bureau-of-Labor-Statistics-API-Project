"""==========================================================================================
    Title:       BLS API ETL Pipeline
    File:        main.py
    Author:      Dan Sagher
    Date:        03/11/2025
    Description:
        This script implements an ETL (Extract, Transform, Load) pipeline that interacts
        with the Bureau of Labor Statistics (BLS) API. It reads a CSV file containing series
        IDs, extracts data from the API, transforms the data into a clean format, and loads
        it into a PostgreSQL database.

    Dependencies:

        External:
            - argparse
            - csv
            - datetime
            - logging
            - os
            - subprocess
            - sys
            - typing
        Internal:
            - api_bls (BlsApiCall class)

    Special Notes:
        - Supports both command-line argument parsing and interactive user input.
        - Implements robust error handling and logging for production-level reliability.
=========================================================================================="""
import argparse as ap
import csv
import datetime as dt
import logging
import os
import subprocess
import sys
from typing import Tuple

from api_bls import BlsApiCall

def arg_parser():
    """
    Parse command-line arguments for the BLS API ETL Pipeline.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments including:
            - path: CSV file path with series IDs.
            - series_type: 1 for National Series or 2 for State Series.
            - start_year: The beginning year for data extraction.
            - end_year: The ending year for data extraction.
            - series_count: Optional limit on the number of series IDs to process.
            - ping: Number of pings to test connectivity to the BLS API.
            - traceroute: Flag to perform a traceroute to the BLS API.
            - verbose: Flag to enable detailed logging output.
            - output: Flag to generate CSV output.
            - silence: Flag to disable logging entirely.
    """
    parser = ap.ArgumentParser(
        prog = 'Bureau of Labor Statistics API Pipeline',
        description = 'This program uses a list of seriesIDs, start year, and end year as input \
                        to the Bureau of Labor Statistics API. The results are parsed, cleaned, and \
                        uploaded into a local PostgreSQL database.',
        usage='python scripts/main.py --csv-path path/to/csv --series-type 1 --start-year 2000 --end-year -n 100 -pov')
    
    parser.add_argument('--path', type=str, help="Path to CSV file containing seriesIDs", default=False,metavar='path/to/csv')
    parser.add_argument('--series-type', type=int,choices=[1,2], help="Enter 1 for National Series or 2 for State Series", default=False,metavar="type")
    parser.add_argument('--start-year', type=int, help="Enter start year for query.", default=False,metavar="year")
    parser.add_argument('--end-year', type=int, help="Enter end year for query.", default=False,metavar="year")
    parser.add_argument('-n', '--series-count', type=int, help="Enter number of seriesIDs to input", default=None, metavar="# of series")
    parser.add_argument('-p','--ping', type=int, help="Enter number of pings to send to the BLS API. Will exit program after execution.", default=False, metavar="# of pings")
    parser.add_argument('-t', '--traceroute', help="Check routing to the BLS API. Will exit program after execution.", action='store_true', default=False)
    parser.add_argument('-v','--verbose', help="Include more information to logging output (Set level to debug).", action='store_true', default=False)
    parser.add_argument('-o','--output', help="Flag to generate CSV output of results", action='store_true', default=False)
    parser.add_argument('-s','--silence', help="Turn off logging to console and file. Takes precedent over --output.", action='store_true', default=False)

    return parser.parse_args()

def interactive_user_input() -> dict:
    """
    Gather user input interactively via the console.
    
    The user is prompted for:
        - CSV file path
        - Series type (National or State)
        - Start and end years for data extraction
        - Optional series count limit
    
    Returns:
        dict: A dictionary containing the user's input.
    """
    print("==========================Interactive Input=============================")
    print("========================================================================", '\n')

    while True:
        path = input("Enter CSV path: ")
        path_valid, message = validate_path(path)
        if path_valid:
            break
        print(message)
    
    while True:
        series_type = int(input("Enter 1 for National Series, 2 for State Series: "))
        if series_type in [1,2]:
            break
        print("Please enter 1 or 2 for series type")

    while True:
        start_year = int(input('Enter start year: '))
        end_year = int(input('Enter end year: '))
        years_valid, message = validate_years(start_year, end_year)
        if years_valid:
            break
        print(message)

    while True:
        series_count = input('Enter desired number of series from input (Press enter for all): ')
        if series_count == '':
            break
        if int(series_count) > 0:
            break
        print("Enter a valid number of years or press enter for all.")
    
    output = {'path':path, 
              'start_year':start_year,
              'end_year':end_year,
              'series_type':series_type,
              'series_count':series_count}
    
    return output

def validate_path(path: str) -> Tuple[bool, str | None]:
    """
    Check if the provided file path exists.
    
    Args:
        path (str): The file path to check.
        
    Returns:
        Tuple[bool, str | None]: A tuple containing a boolean indicating validity,
        and an error message if invalid.
    """
    if os.path.exists(path):
        return True, None
    else:
        return False, "Path not found."
    
def validate_years(start_year: int, end_year: int) -> Tuple[bool, str|None]:
    """
    Validate the start and end years for the data extraction.
    
    Conditions:
        - Both years must be positive integers.
        - The end year cannot be in the future.
        - The start year must precede the end year.
        - The range between years cannot exceed 20 years.
    
    Args:
        start_year (int): The starting year.
        end_year (int): The ending year.
        
    Returns:
        Tuple[bool, str]: A tuple with a boolean indicating validity and an error message if invalid.
    """
    this_year = int(dt.datetime.strftime(dt.datetime.now(), "%Y"))
    try:
        start_year = int(start_year)
        end_year = int(end_year)
    except ValueError:
        return False, "Error: Years must be integers."
    if start_year <= 0 or end_year <= 0:
        return False, "Error: Years must be positive integers."
    elif end_year > this_year:
        return False, f"Error: End year cannot be in the future (current year: {this_year})."
    elif start_year > end_year:
        return False, "Error: Start year must be before end year."
    if end_year - start_year > 20:
        return False, "Error: Year range cannot exceed 20 years due to API limitations."
    else:
        return True, None
    
def read_file(path: str) -> list[dict]:
    """
    Read and parse a CSV file containing series IDs.
    
    Args:
        path (str): Path to the CSV file.
        
    Returns:
        list[dict]: A list of dictionaries representing each row in the CSV.
    """
    with open(path, 'r') as file:
        lst = []
        reader = csv.DictReader(file)
        for row in reader:
            lst.append(row)
        return lst
    
def setup_logging(verbose: bool, output: bool, silence: bool) -> logging.Logger:
    """
    Configure and return a logger for the ETL pipeline.
    
    Args:
        verbose (bool): If True, set the logging level to DEBUG.
        output (bool): If True, add a stream handler for stdout logging.
        silence (bool): If True, disable all logging.
        
    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger("main")

    if not silence:
        format = '%(levelname)s - %(asctime)s - %(message)s'
        formatter = logging.Formatter(format)

        if verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        if output:
            stream_handler = logging.StreamHandler(stream=sys.stdout)
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

        file_handler = logging.FileHandler(filename='outputs/runtime_output/main.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def main() -> None:
    """
    Orchestrate the ETL pipeline by processing user input, validating parameters,
    and invoking the BlsApiCall class to extract, transform, and load BLS API data.
    
    Workflow:
        - Parse command-line arguments.
        - Configure logging.
        - Determine if command-line or interactive input is used.
        - Validate file path and year parameters.
        - Read the CSV file containing series IDs.
        - Instantiate the BlsApiCall object based on the series type.
        - Sequentially perform extraction, transformation, and loading.
    
    Raises:
        FileNotFoundError: If the CSV file path is invalid.
        ValueError: If year validation fails or required arguments are missing.
    """
    args = arg_parser()
    logger = setup_logging(args.verbose, args.output, args.silence)

    if args.ping:
        subprocess.run(args=['ping', '-c', str(args.ping), "https://api.bls.gov/publicAPI/v2/timeseries/data/"], stdout=sys.stdout)
        sys.exit()
    
    if args.traceroute:
        subprocess.run(args=['traceroute', "https://api.bls.gov/publicAPI/v2/timeseries/data/"], stdout=sys.stdout)
        sys.exit()

    if (args.start_year or args.end_year or args.series_type or args.path) and not \
            (args.start_year and args.end_year and args.series_type and args.path):
        logger.error("Error: Not enough information provided in Command Line Arguments.")
        raise ValueError("Please Specify --path, --type, --start-year, and --end-year or nothing for interactive input.")
    
    if args.path and args.series_type and args.start_year and args.end_year:
        logging.debug("Using command line arguments")
        user_input = {
            "path": args.path,
            "series_type": args.series_type,
            "start_year": args.start_year,
            "end_year": args.end_year,
            "series_count": args.series_count or ""}
    else:
        logging.info("Using interactive input")
        user_input = interactive_user_input()

    path_valid, path_message = validate_path(user_input['path'])
    years_valid, years_message = validate_years(user_input['start_year'], user_input['end_year'])
    logger.debug("User input valid")
    
    if not path_valid:  
        logger.error(path_message)
        print(path_message)
        raise FileNotFoundError(path_message)
    
    if not years_valid:
        logger.error(years_message)
        print(years_message)
        raise ValueError(years_message)
    
    try:
        series_input = read_file(user_input['path'])
        start_year = user_input['start_year']
        series_type = user_input['series_type']
        end_year = user_input['end_year']
        series_count = user_input['series_count']
        logger.debug("Successfully read CSV")
    except (csv.Error, PermissionError, ValueError, IOError, UnicodeDecodeError) as e:
        error_message = f"Error reading CSV file: {str(e)}"
        logger.error(error_message)
        print(error_message)
        sys.exit(1)
    except Exception as e:
        error_message = f"Error reading CSV file: {str(e)}"
        logger.error(error_message)
        print(error_message)
        sys.exit(1)

    try:
        if series_type == 1:
            logging.debug("Processing national series")
            api_engine = BlsApiCall(start_year, end_year, national_series=series_input, series_count=series_count)
        else:
            logging.debug("Processing state series")
            api_engine = BlsApiCall(start_year, end_year, state_series=series_input, series_count=series_count)
    except BaseException as e:
        logger.error(f"Error while instantiating BlsApiCall class: {e}")
        raise

    try:
        logger.debug("Extracting data from BLS API")
        api_engine.extract()
        logger.debug("Successfully extracted data from BLS API")
    except BaseException as e:
        logger.error(f"Error while extracting data: {e}")
        raise

    try:
        logger.debug("Transforming and cleaning data")
        api_engine.transform()
        logger.debug("Successfully transformed and cleaned data")
    except BaseException as e:
        logger.error(f"Error while transforming data: {e}")
        raise

    try:
        logger.debug("Loading data into database")
        api_engine.load()
        logger.debug("Successfully loaded data into database")
    except BaseException as e:
        logger.error(f"Error while loading data: {e}")
        raise

if __name__ == "__main__":
    main()
