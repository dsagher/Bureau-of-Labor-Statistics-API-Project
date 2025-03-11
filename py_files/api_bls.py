"""==========================================================================================
    Title:       <>
    File:        <>
    Author:      <Dan Sagher>
    Date:        <>
    Description:


    <>

    Dependencies:

        External:

        Internal:


    Special Concerns:

    API Notes

        - Version 2.0 (10/16/2014)
        - User registration is now required for use of the Public Data API and its new features. 
        - Users must provide an email and organization name during registration.
        - API 2.0 returns up to 20 years of data for up to 50 time series, 
        - with a daily limit of 500 queries.
        - Net and percent calculations are available for one month, two months, six months, twelve months.
        - Annual averages are available.
        - Series description information (catalog) is available for a limited number of series. 
        - (Only surveys included in the BLS Data Finder tool are available for catalog information.)

#=========================================================================================="""
import datetime
from http import HTTPStatus
import json
from itertools import batched
import logging
import pandas as pd
import psycopg2 as pg
from requests.exceptions import HTTPError
import requests
import re as re
import os
import time

from api_key import API_KEY
from config import host, dbname, user, password, port

FORMAT = '%(levelname)s: %(asctime)s - %(message)s'
logger = logging.getLogger('api_bls.log')
logging.basicConfig(filename='api_bls.log', level=logging.INFO, format=FORMAT, datefmt="%Y:%M:%D %H:%M:%S")

class BlsApiCall:
    """
    Add Doc String
    """
    query_count_file = "query_count.txt"

    def __init__(self, national_series = None, state_series = None, number_of_series = None):
        if state_series is None and national_series is None:
            raise Exception('Argument must only be one series list')
        elif state_series is not None and national_series is not None:
            raise Exception('Argument must only be one series list')
        elif state_series is not None and not isinstance(state_series, pd.DataFrame):
            raise TypeError('BlsApiCall inputs must be Pandas DataFrame')
        elif national_series is not None and not isinstance(national_series, pd.DataFrame):
            raise TypeError('BlsApiCall inputs must be Pandas DataFrame')
        
        self.national_series = national_series
        self.state_series = state_series

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
                    self.last_query_count > 500 and \
                    self.current_query_day - self.first_query_day == 0:
            print('made it here')
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
                - Ex:   {'status': 'REQUEST_SUCCEEDED', 'responseTime': 225, 'message': [], 'Results': 
                        {'series': [
                        {'seriesID': 'SMS01000000000000001', 'data': [
                        {'year': '2020', 'period': 'M12', 'periodName': 'December', 'value': '2022.5', 'footnotes': [{}]}, {}, {}, {}]}]}}
                - Structure: dict[str: str, str: int, str: list, str: dict[str: list[dict[str: list[dict[str:str]]]]]]   
        
        Special Notes:
            - Raises ValueError if the seriesID list passed in is greater than 50
            - Raises ValueError if the range of years passed in is greater than 20
            - Raises Exception is API response code is not "REQUEST_SUCCEEDED"
            - Excepts and retries 500 level status codes 3 times before raising HTTPError
            - Excepts and retries Exceptions 3 times before raising Exception
        """

        URL_ENDPOINT = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        RETRIES = 3
        YEAR_LIMIT = 20
        SERIES_LIMIT = 50

        year_range = int(end_year) - int(start_year)
        headers = {"Content-Type": "application/json"}
        payload = json.dumps({"seriesid": series, "startyear": start_year, "endyear": end_year, "registrationKey": API_KEY})

        retry_codes = [HTTPStatus.INTERNAL_SERVER_ERROR, HTTPStatus.BAD_GATEWAY, HTTPStatus.SERVICE_UNAVAILABLE, HTTPStatus.GATEWAY_TIMEOUT]
        
        if len(series) > SERIES_LIMIT:
            raise ValueError("Can only take up to 50 seriesID's per query.")
        elif year_range > YEAR_LIMIT:
            raise ValueError("Can only take in up to 20 years per query.")

        for attempt in range(1, RETRIES + 1):
            try:
                self._create_query_file()
                self._increment_query_count()

                response = requests.post(URL_ENDPOINT, data=payload, headers=headers)
                if self.national_series is not None:
                    logging.info('Request #%s: %s National SeriesIDs, from %s to %s', self.last_query_count, len(series), start_year, end_year)
                if self.state_series is not None:
                    logging.info('Request #%s: %s State SeriesIDs, from %s to %s', self.last_query_count, len(series), start_year, end_year)
                
                if response.status_code.value != HTTPStatus.OK.value:
                    raise HTTPError(response=response.status_code)
                elif response.status_code == HTTPStatus.OK.value and response_status != "REQUEST_SUCCEEDED":
                    logging.warning('Request #%s: Status Code: %s Response: %s', self.last_query_count, response.status_code.value, response_status)
                    raise Exception
                elif response.status_code == HTTPStatus.OK.value and response_status == "REQUEST_SUCCEEDED": 
                    logging.info('Request #%s: Status Code: %s Response: %s', self.last_query_count, response.status_code.value, response_status)
                    response_json = response.json()
                    response_status = response_json["status"]
                    return response_json

            except HTTPError as e:
                if e.response in retry_codes:
                    final_error = e.response
                    logging.warning('HTTP Error: %s Attempt: %s', e.response, attempt)
                    # time.sleep(2**attempt)
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

    def extract(self, start_year: int = 2000, end_year: int = 2002,) -> list:
        #! Move this to documentation
        """
        Feeds list of seriesID's into bls_request() method in batches of 50 or less.

        Parameters:
            - series_df: Pandas DataFrame of any size containing seriesID's
            - start_year: desired start year for query (default 2000)
            - end_year: desired end year for query (default 2002)

        Returns:
            - lst_of_queries: list of response JSONs from bls_request()
                Ex:     [
                            {'status': 'REQUEST_SUCCEEDED', 'responseTime': 225, 'message': [], 'Results': 
                            {'series': 
list no longer than 50 IDs --> [
                            {'seriesID': 'SMS01000000000000001', 'data': [
                            {'year': '2020', 'period': 'M12', 'periodName': 'December', 
                            'value': '2022.5', 'footnotes': [{}]}, 
                            {next_periods/years}...]}
                                ]}},
     start of new query --> {'status': 'REQUEST_SUCCEEDED', 'responseTime': 225, 'message': [], 'Results': 
                            {'series': [
                            {'seriesID': '123456789', 'data': [
                            {'year': '2020', 'period': 'M12', 'periodName': 'December', 
                            'value': '2022.5', 'footnotes': [{}]}, 
                            {next_periods/years}...]}]}}
                        ]

        """

        BATCH_SIZE: int = 50
        INPUT_AMOUNT: int = self.number_of_series
        start_year: str = str(start_year)
        end_year: str = str(end_year)
        self.lst_of_queries: list = []

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

    def _drop_nulls_and_duplicates(self, df):
        df = df.drop_duplicates(subset=['seriesID', 'year', 'period'],keep='first', ignore_index=True)
        df = df.dropna(axis=0)
        return df

    def _values_to_floats(self, df):
        df['value'] = df['value'].replace('-', None)
        df['value'] = df['value'].astype('float')
        return df
    
    def _remove_space(self, df):
        for col in df.columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)     
        return df

    def _convert_adjusted(self,series):
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
        
        series['is_adjusted'] = series['series'].apply(adjusted_column)
        series['series'] = series['series'].apply(remove_terms)

        return series
    
    def transform(self) -> pd.DataFrame:
        """
        Wrangles JSON response into Pandas DataFrame.

        Parameters:
            - final_response_json_lst: list of JSON responses from BLS API
        Returns:
            - final_df: Pandas DataFrame of final results
        """
        final_dct_lst = []
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

        final_df = pd.DataFrame(final_dct_lst)
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

    #! Get this to port to database without coming from Excel outputs first
    #! Could use sql alchemy
    def sql_push(self) -> None:

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

    state_series_path = os.path.join(os.getcwd(), 'outputs/state_scrape_op/state_series_dimension.csv')
    state_series = pd.read_csv(state_series_path)

    national_series_path = os.path.join(os.getcwd(), 'outputs/excel_op/national_series_dimension_og.csv')
    national_series = pd.read_csv(national_series_path)
    print(national_series)
    bad_national_series = national_series[national_series['seriesID'] == 'SMU12000001000000001']
    # bad_national_series = pd.DataFrame([{'seriesID':'SMU12006901000000001'}])

    call_engine = BlsApiCall(national_series, number_of_series=100)
    call_engine.extract(start_year=2000, end_year=2001)
    df = call_engine.transform()
    # df.to_excel('outputs/excel_op/test_excel.xlsx')
