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
import pandas as pd
import datetime as dt
import os

def main() -> None:
    """
    Handles user input and calls ETL functions from BlsApiCall class.
    """
    now = dt.datetime.now().strftime("%d-%b-%Y-%H-%M-%S")
    output_path = os.path.normcase(os.path.join(os.getcwd(), 'outputs/main_output'))

    user_path = input("Enter CSV path: ")
    national_or_state = int(input("Enter 1 for National Series, 2 for State Series: "))
    start_year = int(input('Enter start year: '))
    end_year = int(input('Enter end year: '))
    number_of_series = input('Enter desired number of series from input (Press enter for all): ')

    df = pd.read_csv(user_path) # read_csv will be replaced by open() context manager
    loop = True

    while loop:
        if national_or_state == 1:
            api_engine = BlsApiCall(start_year, end_year, national_series=df, number_of_series=number_of_series)
            name_of_file = f'national_series_{start_year}-{end_year}_{now}' # Deprecated
            full_path = os.path.normcase(os.path.join(output_path, name_of_file)) # Deprecated
            loop = False
        elif national_or_state == 2:
            api_engine = BlsApiCall(start_year, end_year, state_series=df, number_of_series=number_of_series)
            name_of_file = f'state_series_{start_year}-{end_year}_{now}' # Deprecated
            full_path = os.path.normcase(os.path.join(output_path, name_of_file)) # Deprecated
            loop = False
        else:
            print('Please enter 1 for National Series or 2 for State Series')

    api_engine.extract()
    api_engine.transform()
    api_engine.load()

if __name__ == "__main__":
    main()
