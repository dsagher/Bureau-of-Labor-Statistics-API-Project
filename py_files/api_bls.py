"""==========================================================================================

    File:        api_bls.py
    Author:      Dan Sagher
    Date:        3/11/25
    Description:
        This module contains functions to extract, transform, and load data from the Bureau of
        Labor Statistics (BLS) API.

    Dependencies:

        External:
        - datetime
        - http
        - json
        - itertools (batched)
        - logging
        - pandas
        - psycopg2
        - requests
        - requests.exceptions
        - re
        - os
        - time

        Internal:
        - api_key
        - config

    API Notes:
        - Version 2.0 (10/16/2014)
        - User registration is required to access the Public Data API and its new features.
        - Users must provide an email and organization name during registration.
        - API 2.0 supports up to 20 years of data for up to 50 time series, with a daily limit of 500 queries.
        - Net and percent calculations are available for one, two, six, and twelve months.
        - Annual averages are available.
        - Series description information (catalog) is available for a limited number of series, 
          with only surveys included in the BLS Data Finder tool providing catalog information.
    
    Special Notes:
        - Config files will be replaced with environmental variables.
        - Psycopg2 will be replaced with SQLAlchemy ORM for load function.
        - Functionality will be added to be able to take in state and national series concurrently.

=========================================================================================="""
import datetime
from http import HTTPStatus
import json
from itertools import batched
import logging
from pandas import DataFrame, read_csv
import psycopg2 as pg
from requests.exceptions import HTTPError
import requests
import re as re
import os
import time

from api_key import API_KEY
from config import host, dbname, user, password, port

FORMAT = '%(levelname)s - %(asctime)s - %(message)s'
logger = logging.getLogger('api_bls.log')
logging.basicConfig(filename= 'outputs/runtime_output/api_bls.log', level=logging.INFO, format=FORMAT, datefmt="%Y-%M-%D %H-%M-%S")

class BlsApiCall:
    """
    This class contains the functions necessary to extract, transform and load data from the Bureau of
    Labor Statistics (BLS) API.

    Parameters:
        - start_year = User specified desired start year of query
        - end_year = User specified desired end year of query
        - national_series = Pandas DataFrame of National seriesIDs
        - state_series = Pandas DataFrame of State seriesIDs
        - number_of_series = User specified number of seriesIDs to query. Defaults to entire list

    Special Notes:
        - Raises Exception if neither national_series or state_series are input.
        - Raises Exception if both national_series and state_series are input.
        - Raises TypeError if series inputs are not Pandas DataFrames.
    """
    query_count_file = "outputs/runtime_output/query_count.txt"

    def __init__(self, start_year: int, end_year: int, national_series: DataFrame = None, state_series: DataFrame = None, number_of_series: int = None):

        self.start_year: int = start_year
        self.end_year: int = end_year

        if state_series is None and national_series is None:
            raise Exception('Argument must only be one series list')
        elif state_series is not None and national_series is not None:
            raise Exception('Argument must only be one series list')
        elif state_series is not None and not isinstance(state_series, DataFrame):
            raise TypeError('BlsApiCall inputs must be Pandas DataFrame')
        elif national_series is not None and not isinstance(national_series, DataFrame):
            raise TypeError('BlsApiCall inputs must be Pandas DataFrame')
        elif start_year > end_year:
            raise Exception('Start year must be before end year.')
        elif start_year <= 0 or end_year <= 0:
            raise Exception('Please enter a valid year')
        elif end_year > int(datetime.datetime.strftime(datetime.datetime.now(), "%Y")):
            raise Exception("Please enter a valid year.")
        
        self.national_series: DataFrame = national_series
        self.state_series: DataFrame = state_series

        if self.state_series is None:
            self.number_of_series = number_of_series if number_of_series is not None else len(national_series)
        if self.national_series is None:
            self.number_of_series = number_of_series if number_of_series is not None else len(state_series)

        
    def _read_query(self) -> None:
        """
        Method called in create_query_file() and increment_query_count() to read first and last queries.

        Special Note:
            - UnboundLocalError is raised when there is only 1 entry in 'query_count.txt', in which case, 
              self.count and self.day are initialized to self.first_query_count and self.first_query_day
        """

        if os.path.exists(self.query_count_file):

            with open(self.query_count_file, "r") as file:

                self.first_query_count, self.first_query_day = file.readline().split(',')
                self.first_query_count = int(self.first_query_count)
                self.first_query_day = int(self.first_query_day)
       
                try:
                    for line in file:
                        last_line = line
                    self.last_query_count, self.last_query_day = tuple(last_line.split(","))
                    self.last_query_count = int(self.last_query_count)
                    self.last_query_day = int(self.last_query_day)

                except UnboundLocalError: # If there is only one entry
                    self.last_query_count = int(self.first_query_count)
                    self.last_query_day = int(self.first_query_day)

    def _create_query_file(self) -> None:
        """
        Method called in bls_request that creates query count file if one does not exist and deletes file if program
        is run again next day or later.

        Special Notes
            - Sets self.just_created to True after initial creation and False each time program is run after initial creation.
        """

        self._read_query()

        self.current_query_day = int(datetime.datetime.strftime(datetime.datetime.now(), "%d"))
        self.just_created = False 
        
        if os.path.exists(self.query_count_file) and self.current_query_day - self.first_query_day > 0:
            os.remove(self.query_count_file)    

        if not os.path.exists(self.query_count_file):
            self.first_query_day= int(datetime.datetime.strftime(datetime.datetime.now(), "%d"))
            self.count = 1

            with open(self.query_count_file, "w") as file:
                entry = f"{self.count}, {self.first_query_day}\n"
                file.write(str(entry))
            self.just_created = True
    
    def _increment_query_count(self) -> None:
        """
        Increments query count by 1 each time bls_request is called after initial creation.

        Special Notes
            - Raises Exception if query count exceeds 500 within same day.
        """
        self._read_query()
        self.current_query_day = int(datetime.datetime.strftime(datetime.datetime.now(), "%d"))
        if os.path.exists(self.query_count_file) and \
                    self.last_query_count >= 500 and \
                    self.current_query_day - self.first_query_day == 0:
            logging.critical('Queries may not exceed 500 within a day.')
            raise Exception("Queries may not exceed 500 within a day.")
        
        if not self.just_created:
            with open(self.query_count_file, "a") as file:
                file.write(f"{self.last_query_count + 1}, {self.current_query_day}\n")

    def bls_request(self, series: list, start_year: str, end_year: str) -> dict[str|int|list|dict[str,list[dict[str, list[dict[str, str]]]]]]:
        """
        Method called in extract() that sends POST request to Bureau of Labor Statistics API and returns JSON if no errors are encountered.

        Parameters:
            - series: list of seriesID's to send to API
            - start_year: string value of desired start year for query
            - end_year: string value of desired end year for query
        
        Returns:
            - response_json: Dictionary of JSON response from API.
        
        Special Notes:
            - Raises ValueError if the seriesID list passed in is greater than 50
            - Raises ValueError if the range of years passed in is greater than 20
            - Raises Exception is API response code is not "REQUEST_SUCCEEDED"
            - Excepts and retries 500 level status codes 3 times before raising HTTPError
            - Excepts and retries Exceptions 3 times before raising Exception
        """
        URL_ENDPOINT: str = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        RETRIES: int = 3
        YEAR_LIMIT: int = 20
        SERIES_LIMIT: int = 50
        year_range: int = int(end_year) - int(start_year)
        headers: str = {"Content-Type": "application/json"}
        payload: dict = json.dumps({"seriesid": series, "startyear": start_year, "endyear": end_year, "registrationKey": API_KEY})

        retry_codes: list = [HTTPStatus.INTERNAL_SERVER_ERROR, HTTPStatus.BAD_GATEWAY, HTTPStatus.SERVICE_UNAVAILABLE, HTTPStatus.GATEWAY_TIMEOUT]
        
        if len(series) > SERIES_LIMIT:
            raise ValueError("Can only take up to 50 seriesID's per query.")
        elif year_range > YEAR_LIMIT:
            raise ValueError("Can only take in up to 20 years per query.")
       
        for attempt in range(1, RETRIES + 1):

            self._create_query_file()
            self._increment_query_count()

            try:
            
                response = requests.post(URL_ENDPOINT, data=payload, headers=headers)
  
                if self.national_series is not None:
                    logging.info('Request #%s: %s National SeriesIDs, from %s to %s', self.last_query_count, len(series), start_year, end_year)
                if self.state_series is not None:
                    logging.info('Request #%s: %s State SeriesIDs, from %s to %s', self.last_query_count, len(series), start_year, end_year)
                
                if response.status_code != HTTPStatus.OK.value:
                    raise HTTPError(response=response.status_code)
                elif response.status_code == HTTPStatus.OK.value:
                    response_json = response.json()
                    response_status = response_json["status"]
                
                    if response_status != "REQUEST_SUCCEEDED":
                        logging.warning('Request #%s: Status Code: %s Response: %s', self.last_query_count, response.status_code, response_status)
                        raise Exception
                    else:
                        logging.info('Request #%s: Status Code: %s Response: %s', self.last_query_count, response.status_code, response_status)
                        return response_json

            except HTTPError as e:
                if e.response in retry_codes:
                    final_error = e.response
                    logging.warning('HTTP Error: %s Attempt: %s', e.response, attempt)
                    time.sleep(2**attempt)
                    continue
                else:
                    final_error = e.response
                    logging.critical('HTTP Error: %s', e.response)
                    raise HTTPError(f"HTTP Error: {e.response}")
                
            except Exception as e:
                logging.critical('Response Status from API is not "REQUEST_SUCCEEDED"')
                raise Exception('Response Status from API is not "REQUEST_SUCCEEDED"')

        logging.critical('API Error: %s', final_error)
        raise Exception(f"API Error: {final_error}")

    def extract(self) -> list:
        """
        Feeds list of seriesID's into bls_request() method in batches of 50 or less.

        Parameters:
            - series_df: Pandas DataFrame of any size containing seriesID's
            - start_year: desired start year for query (default 2000)
            - end_year: desired end year for query (default 2002)

        Returns:
            - lst_of_queries: list of response JSONs from bls_request()
        """
        BATCH_SIZE: int = 50
        INPUT_AMOUNT: int = self.number_of_series
        start_year: str = str(self.start_year)
        end_year: str = str(self.end_year)
        self.lst_of_queries: list[dict] = []
        
        if self.national_series is not None:
            series_id_lst: list = list(self.national_series["seriesID"])[:INPUT_AMOUNT]
        elif self.state_series is not None:
            series_id_lst: list = list(self.state_series["seriesID"])[:INPUT_AMOUNT]

        batch_progress = 0
        for batch in batched(series_id_lst, BATCH_SIZE):
            batch = list(batch)
            batch_size = len(batch)
            batch_progress += batch_size
            total_size = len(series_id_lst)

            print(f"Processessing batch of size: {batch_size}")
            print(f'Progress: {batch_progress}/{total_size} {(batch_progress/total_size):.0%}')

            result = self.bls_request(batch, start_year, end_year)
            self.lst_of_queries.append(result)
            time.sleep(0.25)
            
        return self.lst_of_queries

    def _log_message(self, messages: str) -> None:
        """
        Method looped in transform() that extracts the message and year that appear in response JSON
        when year is missing information for a specific seriesID

        Parameters:
            - message: string value of message in response JSON
        
        Special Concerns:
            - Planning to add functionality to log successful queries.
        """
        for message in messages:
            year_reg_match = re.fullmatch(r'No Data Available for Series (\w+) Year: (\d\d\d\d)', message)
            no_series_reg_match = re.fullmatch(r'Series does not exist for Series (\w+)', message)
            if year_reg_match:
                series_id, year = year_reg_match.group(1,2)
                msg = 'No Data Available'
                entry = {'message': msg, "series_id": series_id, "year": year}
                logging.warning(entry)
            elif no_series_reg_match:
                series_id = no_series_reg_match.group(1)
                msg = 'Series does not exist'
                entry = {'message': msg, "series_id": series_id}
                logging.warning(entry)
            else:
                logging.warning(message)

    def _drop_nulls_and_duplicates(self, df: DataFrame) -> DataFrame:
        df = df.drop_duplicates(subset=['seriesID', 'year', 'period'],keep='first', ignore_index=True)
        df = df.dropna(axis=0)
        return df

    def _values_to_floats(self, df: DataFrame) -> DataFrame:
        df['value'] = df['value'].replace('-', None)
        df['value'] = df['value'].astype('float')
        return df
    
    def _remove_space(self, df: DataFrame) -> DataFrame:
        for col in df.columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)     
        return df

    def _convert_adjusted(self, df: DataFrame) -> DataFrame:
        """
        Adds boolean column to indicate if a series is seasonally adjusted and removes
        the term from original series name.
        """
        adjusted = ', seasonally adjusted'
        not_adjusted = ', not seasonally adjusted'

        def adjusted_column(text):
            text = str(text).lower()
            return True if text.endswith(adjusted) else False if text.endswith(not_adjusted) else None
        
        def remove_terms(text):
            text_lower = text.lower()
            index1 = text_lower.find(adjusted)
            index2 = text_lower.find(not_adjusted)
            if index1 != -1:
                return text[:index1]
            elif index2 != -1:
                return text[:index2]
            return text
        
        df['is_adjusted'] = df['series'].apply(adjusted_column)
        df['series'] = df['series'].apply(remove_terms)

        return df
    
    def transform(self) -> DataFrame:
        """
        Wrangles JSON response into Pandas DataFrame.

        Parameters:
            - final_response_json_lst: list of JSON responses from BLS API
        Returns:
            - final_df: Pandas DataFrame of final results
        """
        final_dct_lst: list = []
        for response in self.lst_of_queries:
            results = response.get("Results")
            series_dct = results.get("series")

            if response["message"] != []:
                message = response.get("message")
                self._log_message(message)

            for series in series_dct:
                series_id = series["seriesID"]

                for data_point in series["data"]:
                    data_dict = {
                        "seriesID": series_id,
                        "year": data_point["year"],
                        "period": data_point["period"],
                        "period_name": data_point["periodName"],
                        "value": data_point["value"],
                        "footnotes": data_point["footnotes"] if not "[{}]" in data_point else None
                    }

                    final_dct_lst.append(data_dict)

        final_df: DataFrame = DataFrame(final_dct_lst)
        if not final_df.empty:
            if self.national_series is not None:
                final_df = final_df.merge(self.national_series, on='seriesID',how='left')
            elif self.state_series is not None:
                final_df = final_df.merge(self.state_series, on='seriesID', how='left')

            final_df_cleaned = final_df.copy()
            final_df_cleaned = self._drop_nulls_and_duplicates(final_df_cleaned)
            final_df_cleaned = self._values_to_floats(final_df_cleaned)
            final_df_cleaned = self._remove_space(final_df_cleaned)
            final_df_cleaned = self._convert_adjusted(final_df_cleaned)
            
            print("DataFrame Created")

            return final_df_cleaned
        else:
            raise Exception('DataFrame is Empty')

    def load(self) -> None:
        """
        (Deprecated Function)
        Loads data into a PostgreSQL database. 

        Special Concerns:
            - Will be replaced with SQLAlchemy ORM.
            - Will take in data directly from program, not external CSV files.
        """

        path = os.getcwd()

        conn = pg.connect(host=host, dbname=dbname, user=user, password=password, port=port)

        cur = conn.cursor()

        cur.execute(
            """
                    --sql
                    CREATE TABLE IF NOT EXISTS state_series (
                    seriesID VARCHAR PRIMARY KEY,
                    series VARCHAR,
                    state VARCHAR,
                    survey VARCHAR,
                    is_adjusted BOOLEAN
                    );

                    --sql
                    CREATE TABLE IF NOT EXISTS national_series (
                    seriesID VARCHAR PRIMARY KEY,
                    series VARCHAR,
                    survey VARCHAR,
                    is_adjusted BOOLEAN
                    );

                    --sql
                    CREATE TABLE IF NOT EXISTS state_results (
                    seriesID VARCHAR,
                    year INT,
                    period VARCHAR,
                    period_name VARCHAR,
                    value FLOAT,
                    footnotes VARCHAR
                    );

                    --sql
                    CREATE TABLE IF NOT EXISTS national_results (
                    seriesID VARCHAR,
                    year INT,
                    period VARCHAR,
                    period_name VARCHAR,
                    value FLOAT,
                    footnotes VARCHAR
                    );

                    --sql
                    CREATE TABLE IF NOT EXISTS survey_table (
                    survey VARCHAR, 
                    survey_name VARCHAR
                    );
                    
                    """
        )

        cur.execute(
            f"""
                    --sql
                    COPY national_series
                    FROM '{path}/outputs/cleaning_op/national_series_dimension_cleaned.csv' DELIMITER ',' CSV HEADER;

                    --sql
                    COPY state_series
                    FROM '{path}/outputs/cleaning_op/state_series_dimension_cleaned.csv' DELIMITER ',' CSV HEADER;

                    --sql
                    COPY national_results
                    FROM '{path}/outputs/cleaning_op/national_results_cleaned.csv' DELIMITER ',' CSV HEADER;

                    --sql
                    COPY state_results
                    FROM '{path}/outputs/cleaning_op/state_results_cleaned.csv'  DELIMITER ',' CSV HEADER;

                    --sql
                    COPY survey_table
                    FROM '{path}/outputs/excel_op/survey_table.csv' DELIMITER ',' CSV HEADER;
        """
        )

        conn.commit()
        cur.close()
        conn.close()

if __name__ == "__main__":

    state_series_path = os.path.join(os.getcwd(), 'inputs/state_series_dimension.csv')
    state_series = read_csv(state_series_path)
    national_series_path = os.path.join(os.getcwd(), 'inputs/national_series_dimension.csv')
    national_series = read_csv(national_series_path)

    call_engine = BlsApiCall(2000, 2005,national_series=national_series, number_of_series=250)
    call_engine.extract()
    df = call_engine.transform()
