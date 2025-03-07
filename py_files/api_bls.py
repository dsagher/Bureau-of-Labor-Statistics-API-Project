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

#=========================================================================================="""

"""
Version 2.0 (10/16/2014)
User registration is now required for use of the Public Data API and its new features. 
Users must provide an email and organization name during registration.
API 2.0 returns up to 20 years of data for up to 50 time series, 
with a daily limit of 500 queries.
Net and percent calculations are available for one month, two months, six months, twelve months.
Annual averages are available.
Series description information (catalog) is available for a limited number of series. 
(Only surveys included in the BLS Data Finder tool are available for catalog information.)
"""
import pandas as pd
import requests
import json
from api_key import API_KEY
import time
import datetime
from requests.exceptions import HTTPError
import pandas as pd
import psycopg2 as pg
from config import host, dbname, user, password, port
import pprint as pp
from http import HTTPStatus
import logging
import os
from itertools import batched


#! Could read these in without pandas 
state_series_path = os.path.join(os.getcwd(), 'outputs/cleaning_op/state_series_dimension_cleaned.csv')
state_series = pd.read_csv(state_series_path)

national_series_path = os.path.join(os.getcwd(), 'outputs/cleaning_op/national_series_dimension_cleaned.csv')
national_series = pd.read_csv(state_series_path)


class BlsApiCall:
    """
    Add Doc String
    """

    query_count_file = "query_count.txt"

    def __init__(self):

        pass

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
                    self.day = int(self.last_query_day)

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
        payload = json.dumps({"seriesid": series,
                              "startyear": start_year,
                              "endyear": end_year,
                              "registrationKey": API_KEY})
        retry_codes = [HTTPStatus.INTERNAL_SERVER_ERROR, 
                       HTTPStatus.BAD_GATEWAY, 
                       HTTPStatus.SERVICE_UNAVAILABLE, 
                       HTTPStatus.GATEWAY_TIMEOUT]

        if len(series) > SERIES_LIMIT:
            raise ValueError("Can only take up to 50 seriesID's per query.")
        elif year_range > YEAR_LIMIT:
            raise ValueError("Can only take in up to 20 years per query.")

        for attempt in range(0, RETRIES):

            try:

                self._create_query_file()
                self._increment_query_count()

                response = requests.post(URL_ENDPOINT, data=payload, headers=headers)

                response_json = response.json()
 
                self.response_status = response_json["status"]

                if (response.status_code == HTTPStatus.OK and self.response_status == "REQUEST_SUCCEEDED"): 
                    return response_json
                elif self.response_status != "REQUEST_SUCCEEDED":
                    raise Exception()

            except HTTPError as e:
                if response.status_code in retry_codes:
                    time.sleep(2**attempt)
                    continue
                else:
                    raise HTTPError(f"HTTP error occurred: {e}")

            except Exception as e:
                time.sleep(2**attempt)
                continue
         

        raise Exception(f"API Error: {response_json['status']}, Code: {response.status_code}")

    def extract(self, series_df: pd.DataFrame, start_year: int = 2000, end_year: int = 2002,) -> list:
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
        start_year: str = str(start_year)
        end_year: str = str(end_year)
        series_id_lst: list = list(series_df["seriesID"])
        lst_of_queries: list = []
    
        for batch in batched(series_id_lst, BATCH_SIZE):

            batch = list(batch)
            print(f"Processessing batch of size: {len(batch)}")
            print(batch)

            result = self.bls_request(batch, start_year, end_year)

            lst_of_queries.append(result)
            time.sleep(0.25)

        return lst_of_queries

    def _set_message_ouput(self, message: str) -> None:
        """
        Method looped in transform() that extracts the message and year that appear in response JSON
        when year is missing information for a specific seriesID

        Parameters:
            - message: string value of message in response JSON
        
        """
        message_lst = []
        message_lst.append(message)

        self.message_df = pd.DataFrame()

        series_id = message[29:-10]
        year = message[-4:]
        new_row = {"series_id": series_id, "year": year}

        pd.concat([self.message_df, pd.Series(new_row).to_frame()])

    def transform(self, final_response_json_lst: list) -> pd.DataFrame:
        """ """
        final_dct_lst = []

        print("Creating DataFrame...")

        for response in final_response_json_lst:
            results = response.get("Results")
            series_dct = results.get("series")

            if response["message"] is not None:
                message = response.get("message")
                #! Configure logging
                logging.info(message)
                self._set_message_ouput(message)

            for series in series_dct:

                series_id = series["seriesID"]

                for data_point in series["data"]:
                    data_dict = {
                        "seriesID": series_id,
                        "year": data_point["year"],
                        "period": data_point["period"],
                        "period_name": data_point["periodName"],
                        "value": data_point["value"],
                        "footnotes": (
                            data_point["footnotes"]
                            if not "[{}]" in data_point
                            else None
                        ),
                    }
                    final_dct_lst.append(data_dict)

        final_df = pd.DataFrame(final_dct_lst)

        print("DataFrame Created")
        return final_df

    #! Get this to port to database without coming from Excel outputs first
    def sql_push(self) -> None:

        PATH = "/Users/danielsagher/Dropbox/Documents/projects/bls_api_project/"

        conn = pg.connect(
            host=host, dbname=dbname, user=user, password=password, port=port
        )

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
                    FROM '{PATH}/outputs/cleaning_op/national_series_dimension_cleaned.csv' DELIMITER ',' CSV HEADER;

                    --sql
                    COPY state_series
                    FROM '{PATH}/outputs/cleaning_op/state_series_dimension_cleaned.csv' DELIMITER ',' CSV HEADER;

                    --sql
                    COPY national_results
                    FROM '{PATH}/outputs/cleaning_op/national_results_cleaned.csv' DELIMITER ',' CSV HEADER;

                    --sql
                    COPY state_results
                    FROM '{PATH}/outputs/cleaning_op/state_results_cleaned.csv'  DELIMITER ',' CSV HEADER;

                    --sql
                    COPY survey_table
                    FROM '{PATH}/outputs/excel_op/survey_table.csv' DELIMITER ',' CSV HEADER;
        """
        )

        conn.commit()
        cur.close()
        conn.close()


if __name__ == "__main__":

    call_engine = BlsApiCall()
    result = call_engine.extract(national_series[:1])
    df = call_engine.transform(result)
    # print(call_engine.first_query)
