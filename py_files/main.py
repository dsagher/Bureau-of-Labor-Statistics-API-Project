"""==========================================================================================

    Title:       <Bureau of Labor Statistics API Pipeline>
    File:        <main.py>
    Author:      <Dan Sagher>
    Date:        <3/11/25>
    Description:
        This pipeline extracts, transforms, and loads data from the Bureau of Labor Statistics
        API into a Postgres database. User uploads an CSV dimension file with series names and IDs.

    Dependencies:

        External:

        - pandas
        - datetime
        - os

        Internal:

        - api_bls


    Special Concerns: 

#=========================================================================================="""

from api_bls import BlsApiCall
import pandas as pd
import datetime as dt
import os

def main() -> None:
    """
    :params:
    :returns:
    """
    now = dt.datetime.now().strftime("%d-%b-%Y-%H-%M-%S")
    output_path = os.path.normcase(os.path.join(os.getcwd(), 'outputs/main_output'))

    user_path = input("Enter CSV path: ")
    national_or_state = int(input("Enter 1 for National Series, 2 for State Series: "))
    start_year = input('Enter start year: ')
    end_year = input('Enter end year: ')
    number_of_series = int(input('Enter desired number of series from input (Press enter for all): '))

    df = pd.read_csv(user_path)
    loop = True

    while loop:
        if national_or_state == 1:
            api_engine = BlsApiCall(start_year, end_year, national_series=df, number_of_series=number_of_series)
            name_of_file = f'national_series_{start_year}-{end_year}_{now}'
            full_path = os.path.normcase(os.path.join(output_path, name_of_file))
            loop = False
        elif national_or_state == 2:
            api_engine = BlsApiCall(start_year, end_year, state_series=df, number_of_series=number_of_series)
            name_of_file = f'state_series_{start_year}-{end_year}_{now}'
            full_path = os.path.normcase(os.path.join(output_path, name_of_file))
            loop = False
        else:
            print('Please enter 1 for National Series or 2 for State Series')

    api_engine.extract()
    api_engine.transform()
    # api_engine.load()
    df.to_csv(full_path)

    return df


if __name__ == "__main__":
    main()
