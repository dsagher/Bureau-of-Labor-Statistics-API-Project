"""==========================================================================================
    Title:       BLS API ETL Module
    File:        api_bls.py
    Author:      Dan Sagher
    Date:        03/11/2025
    Description:
        This module implements the extraction, transformation, and loading (ETL) logic
        for interacting with the Bureau of Labor Statistics (BLS) API. It defines the
        BlsApiCall class which manages API requests, response handling, data cleaning,
        and database insertion using PostgreSQL via SQLAlchemy.

    Dependencies:

        External:
            - copy
            - datetime
            - json
            - logging
            - os
            - re
            - time
            - http.HTTPStatus, 
            - itertools.batched
            - requests
            - requests.exceptions
            - SQLAlchemy (create_engine, MetaData, Table, Column, Integer, String, Boolean, Float, ForeignKey,
              dialects.postgresql.insert, engine.URL)

        Internal:
            - api_key, config

    API Notes:
        - BLS API Version 2.0 (10/16/2014) requires user registration with email and organization.
        - Supports up to 20 years of data for a maximum of 50 series per query, with a daily limit of 500 queries.
        - Provides additional calculations such as net and percent changes, annual averages, and limited series catalog info.
=========================================================================================="""

import copy
import datetime
import json
import logging
import os
import re
import time
from typing import TypedDict, Dict, List
from http import HTTPStatus
from itertools import batched

import requests
from requests.exceptions import HTTPError

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Boolean,
    Float,
    ForeignKey,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL

class DataDict(TypedDict):
    seriesID: str
    year: int
    period: str
    period_name: str
    value: float|None
    footnotes: str|None
    
class SeriesData(TypedDict):
    year: str
    period: str
    periodName: str
    value: str
    footnotes: List[Dict] | str

class Series(TypedDict):
    seriesID: str
    data: List[SeriesData]

class DataContainer(TypedDict):
    series: List[Series]

class BLSResponse(TypedDict):
    status: str
    responseTime: int
    message: List[str]
    Results: DataContainer

class BlsApiCall:
    """
    Manages the ETL process for retrieving data from the BLS API and loading it into a PostgreSQL database.

    Parameters:
        start_year (int): Desired starting year for the query.
        end_year (int): Desired ending year for the query.
        national_series (list[dict], optional): List of dictionaries representing national series IDs.
        state_series (list[dict], optional): List of dictionaries representing state series IDs.
        series_count (int or str, optional): Maximum number of series IDs to process. Defaults to all provided.

    Raises:
        Exception: If both or neither of national_series/state_series are provided.
    """
    query_count_file = "outputs/runtime_output/query_count.txt"

    def __init__(self, start_year: int, end_year: int, national_series: list[dict]= [], state_series: list[dict] = [], series_count: int|str = ""):

        self.logger = logging.getLogger('main.api')

        if len(state_series) != 0 and len(national_series) != 0:
            self.logger.error("Argument must only be one series list")
            raise Exception("Argument must only be one series list")
        elif len(state_series) == 0 and len(national_series) == 0:
            self.logger.error("Argument must only be one series list")
            raise Exception("Argument must only be one series list")

        self.start_year = start_year
        self.end_year = end_year
        self.national_series = national_series
        self.state_series = state_series

        if len(self.state_series) == 0:
            self.series_count = int(series_count) if series_count != '' else len(national_series)
            self.state = False
            self.national = True
        if len(self.national_series) == 0:
            self.series_count = int(series_count) if series_count != '' else len(state_series)
            self.national = False
            self.state = True

    def _read_query(self) -> None:
        """
        Reads the query count file to obtain the first and last query counts and their corresponding days.

        Special Note:
            If only one entry exists in the file, initializes last_query_count and last_query_day to the first values.
        """

        if os.path.exists(self.query_count_file):

            with open(self.query_count_file, "r") as file:

                first_query_count, first_query_day = file.readline().split(',')
                self.first_query_count = int(first_query_count)
                self.first_query_day = int(first_query_day)
       
                try:
                    for line in file:
                        last_line = line
                    last_query_count, last_query_day = tuple(last_line.split(","))
                    self.last_query_count = int(last_query_count)
                    self.last_query_day = int(last_query_day)

                except UnboundLocalError:
                    self.last_query_count = int(first_query_count)
                    self.last_query_day = int(first_query_day)

    def _create_query_file(self) -> None:
        """
        Creates the query count file if it does not exist, or deletes it if the day has changed.
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
        Increments the query count by 1 each time an API request is made.

        Raises:
            Exception: If the daily query limit (500 queries) is exceeded.
        """
        self._read_query()
        self.current_query_day = int(datetime.datetime.strftime(datetime.datetime.now(), "%d"))
        if os.path.exists(self.query_count_file) and \
                    self.last_query_count >= 500 and \
                    self.current_query_day - self.first_query_day == 0:
            self.logger.critical('Queries may not exceed 500 within a day.')
            raise Exception("Queries may not exceed 500 within a day.")
        
        if not self.just_created:
            with open(self.query_count_file, "a") as file:
                file.write(f"{self.last_query_count + 1}, {self.current_query_day}\n")

    def bls_request(self, series: list, start_year: str, end_year: str) -> BLSResponse:
        """
        Sends a POST request to the BLS API for the specified series and date range.

        Parameters:
            series (list): List of series IDs to query.
            start_year (str): Desired start year.
            end_year (str): Desired end year.

        Returns:
            BLSResponse: The JSON response from the API.

        Raises:
            ValueError: If more than 50 series IDs are provided or if the year range exceeds 20.
            HTTPError: If an HTTP error occurs (with retries for specific status codes).
            Exception: If the API response status is not "REQUEST_SUCCEEDED".
        """
        URL_ENDPOINT = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        RETRIES = 3
        YEAR_LIMIT = 20
        SERIES_LIMIT = 50
        RETRY_CODES: list = [HTTPStatus.INTERNAL_SERVER_ERROR, HTTPStatus.BAD_GATEWAY, HTTPStatus.SERVICE_UNAVAILABLE, HTTPStatus.GATEWAY_TIMEOUT]
        year_range = int(end_year) - int(start_year)
        headers = {"Content-Type": "application/json"}
        payload = json.dumps({"seriesid": series, 
                              "startyear": start_year, 
                              "endyear": end_year, 
                              "registrationKey": os.getenv("BLS_API_KEY")})
        
        if len(series) > SERIES_LIMIT:
            raise ValueError("Can only take up to 50 seriesID's per query.")
        elif year_range > YEAR_LIMIT:
            raise ValueError("Can only take in up to 20 years per query.")
       
        for attempt in range(1, RETRIES + 1):
            self._create_query_file()
            self._increment_query_count()

            try:
                response = requests.post(URL_ENDPOINT, data=payload, headers=headers)
                
                if self.national:
                    self.logger.info('Request #%s: %s National SeriesIDs, from %s to %s', self.last_query_count, len(series), start_year, end_year)
                if self.state:
                    self.logger.info('Request #%s: %s State SeriesIDs, from %s to %s', self.last_query_count, len(series), start_year, end_year)
                
                if response.status_code != HTTPStatus.OK.value:
                    raise HTTPError(response=response.status_code)
                elif response.status_code == HTTPStatus.OK.value:
                    response_json = response.json()
                    response_status = response_json["status"]
                
                    if response_status != "REQUEST_SUCCEEDED":
                        self.logger.warning('Request #%s: Status Code: %s Response: %s', self.last_query_count, response.status_code, response_status)
                        raise Exception
                    else:
                        self.logger.info('Request #%s: Status Code: %s Response: %s', self.last_query_count, response.status_code, response_status)
                        return response_json

            except HTTPError as e:
                if e.response in RETRY_CODES:
                    final_error = e.response
                    self.logger.warning('HTTP Error: %s Attempt: %s', e.response, attempt)
                    time.sleep(2**attempt)
                    continue
                else:
                    final_error = e.response
                    self.logger.critical('HTTP Error: %s', e.response)
                    raise HTTPError(f"HTTP Error: {e.response}")
                
            except Exception as e:
                self.logger.critical('Response Status from API is not "REQUEST_SUCCEEDED"')
                self.logger.critical(f"API Message: {response_json['message']}")
                raise Exception('Response Status from API is not "REQUEST_SUCCEEDED"')

        self.logger.critical('API Error: %s', final_error)
        raise Exception(f"API Error: {final_error}")

    def extract(self) -> None:
        """
        Extracts data from the BLS API in batches of up to 50 series IDs.

        The method iterates over the list of series IDs in batches, sends API requests via
        bls_request, and stores the JSON responses.
        """
        BATCH_SIZE = 50
        INPUT_AMOUNT: int = self.series_count
        start_year = str(self.start_year)
        end_year = str(self.end_year)
        self.lst_of_queries: list[BLSResponse] = []

        if self.national:
            series_id_lst = [i.get("seriesID") for i in self.national_series][:INPUT_AMOUNT]
        elif self.state:
            series_id_lst = [i.get("seriesID") for i in self.state_series][:INPUT_AMOUNT]

        batch_progress = 0
        for batch_tuple in batched(series_id_lst, BATCH_SIZE):
            batch = list(batch_tuple)
            batch_size = len(batch)
            batch_progress += batch_size
            total_size = len(series_id_lst)

            self.logger.debug(f"Extracting batch of size: {batch_progress}")
            self.logger.debug(f'Progress: {batch_progress}/{total_size} {(batch_progress/total_size):.0%}')

            result = self.bls_request(batch, start_year, end_year)
            self.lst_of_queries.append(result)
            time.sleep(0.25)
            
        self.logger.info(f"Successfully extracted {total_size} IDs")
        
    def _log_message(self, messages: list[str]) -> None:
        """
        Logs warning messages extracted from the API response when data for a given series/year is missing.

        Parameters:
            messages (list[str]): List of messages from the API response.
        """
        for message in messages:
            year_reg_match = re.fullmatch(r'No Data Available for Series (\w+) Year: (\d\d\d\d)', message)
            no_series_reg_match = re.fullmatch(r'Series does not exist for Series (\w+)', message)
            if year_reg_match:
                series_id, year = year_reg_match.group(1,2)
                msg = 'No Data Available'
                entry = {'message': msg, "series_id": series_id, "year": year}
                self.logger.warning(entry)
            elif no_series_reg_match:
                series_id = no_series_reg_match.group(1)
                msg = 'Series does not exist'
                entry = {'message': msg, "series_id": series_id}
                self.logger.warning(entry)
            else:
                self.logger.warning(message)

    def _drop_nulls_and_duplicates(self, lst: list[DataDict]) -> list[DataDict]:
        """
        Removes duplicate dictionaries from a list.

        Parameters:
            lst (list[dict]): The list of dictionaries to process.

        Returns:
            list[dict]: A new list with duplicates removed.
        """
        unique = []
        for dct in lst:
            if dct in unique:
                continue
            else:
                unique.append(dct)
        return unique

    def _convert_adjusted(self, lst: list[dict]) -> list[dict]:
        """
        Processes a list of series dictionaries to:
            - Add a boolean indicating if a series is seasonally adjusted.
            - Remove adjustment phrases from the series name.
        
        Parameters:
            lst (list[dict]): List of series dictionaries.

        Returns:
            list[dict]: The updated list with an 'is_adjusted' flag and cleaned series names.
        """
        adjusted = ', seasonally adjusted'
        not_adjusted = ', not seasonally adjusted'
        
        def remove_terms(str):
            text_lower = str.lower()
            index1 = text_lower.find(adjusted)
            index2 = text_lower.find(not_adjusted)
            if index1 != -1:
                str = str[:index1]
            elif index2 != -1:
                str = str[:index2]
            return str
    
        for dct in lst:
            if dct['series'].lower().endswith(adjusted):
                dct['is_adjusted'] = True
                dct['series'] = remove_terms(dct['series'])
            elif dct['series'].lower().endswith(not_adjusted):
                dct['is_adjusted'] = False
                dct['series'] = remove_terms(dct['series'])
            else:
                dct['is_adjusted'] = None

        return lst
    
    def transform(self) -> None:
        """
        Transforms the extracted JSON responses into a clean, deduplicated dataset.
        
        This involves:
            - Logging any warnings from the API messages.
            - Flattening nested JSON data into a list of dictionaries.
            - Removing duplicates.
            - Processing series metadata to flag seasonal adjustments.
        """
        final_dct_lst: list[DataDict] = []

        for response in self.lst_of_queries:
            results: DataContainer = response["Results"]
            series_lst: List[Series] = results["series"]

            if response["message"] != []:
                message: list[str] = response["message"]
                self._log_message(message)

            for series in series_lst:
                series_id: str = series["seriesID"]

                for data_point in series["data"]:
                    data_dict: DataDict = {
                        "seriesID": series_id,
                        "year": int(data_point["year"]),
                        "period": data_point["period"],
                        "period_name": data_point["periodName"],
                        "value": float(data_point["value"]) if data_point["value"] != '-' else None,
                        "footnotes": str(data_point["footnotes"]) if data_point["footnotes"] != str([{}]) else None
                    }
                    final_dct_lst.append(data_dict)
        try:
            self.final_dct_lst_copy = copy.deepcopy(final_dct_lst)
        except copy.Error as e:
            logging.error(f"Error copying list of results: {e}")
            raise

        self.final_dct_lst_copy = self._drop_nulls_and_duplicates(self.final_dct_lst_copy)

        if self.national:
            national_series_copy = copy.deepcopy(self.national_series)
            self.national_series_copy = self._convert_adjusted(national_series_copy)
        elif self.state:
            state_series_copy = copy.deepcopy(self.state_series)
            self.state_series_copy = self._convert_adjusted(state_series_copy)
        
    def load(self) -> None:
        """
        Loads the transformed data into the PostgreSQL database using SQLAlchemy.
        
        Reads database configuration from 'inputs/config.json', creates tables for series
        and results if they don't exist, and performs upsert operations to avoid duplicates.
        """
        with open('inputs/config.json', 'r') as file:
            config = json.load(file)
            driver = config['driver']
            username = config['username']
            password = config['password']
            host = config['host']
            database = config['database']
            port = config['port']

        url_object = URL.create(drivername=driver, username=username, password=password, host=host, database=database, port=port)
        engine = create_engine(url_object, logging_name='SQLAlchemy')
        metadata = MetaData()
        state_series = Table('state_series', 
                            metadata, 
                            Column('seriesID', String, primary_key=True),
                            Column('series', String, nullable=False),
                            Column('state', String, nullable=False),
                            Column('survey', String),
                            Column('is_adjusted', Boolean))
        national_series = Table('national_series', 
                            metadata, 
                            Column('seriesID', String, primary_key=True),
                            Column('series', String, nullable=False),
                            Column('survey', String),
                            Column('is_adjusted', Boolean))
        state_results = Table('state_results', 
                            metadata, 
                            Column('seriesID', String, ForeignKey('state_series.seriesID'), primary_key=True),
                            Column('year', Integer, nullable=False, primary_key=True),
                            Column('period', String, nullable=False, primary_key=True),
                            Column('period_name',String, nullable=False),
                            Column('value', Float),
                            Column('footnotes', String))
        national_results = Table('national_results', 
                            metadata, 
                            Column('seriesID', String, ForeignKey('national_series.seriesID'), primary_key=True),
                            Column('year', Integer, nullable=False, primary_key=True),
                            Column('period', String, nullable=False, primary_key=True),
                            Column('period_name',String, nullable=False),
                            Column('value', Float),
                            Column('footnotes', String))

        metadata.create_all(bind=engine, checkfirst=True)

        if self.state:
            series_stmt = insert(state_series).values(self.state_series_copy).on_conflict_do_nothing()
            results_stmt = insert(state_results).values(self.final_dct_lst_copy).on_conflict_do_nothing()
        if self.national:
            series_stmt = insert(national_series).values(self.national_series_copy).on_conflict_do_nothing()
            results_stmt = insert(national_results).values(self.final_dct_lst_copy).on_conflict_do_nothing()
        with engine.connect() as connect:
            connect.execute(series_stmt)
            connect.execute(results_stmt)
            connect.commit()

if __name__ == "__main__":

    from main import read_file

    state_series_path = os.path.join(os.getcwd(), 'inputs/state_series_dimension.csv')
    state_series = read_file(state_series_path)
    national_series_path = os.path.join(os.getcwd(), 'inputs/national_series_dimension.csv')
    national_series = read_file(national_series_path)
    call_engine = BlsApiCall(2000, 2005,national_series=national_series, series_count=1)
    call_engine.extract()
    call_engine.transform()
    call_engine.load()
