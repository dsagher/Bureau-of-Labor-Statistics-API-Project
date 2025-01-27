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
import requests as rq
import json
import api_key
import time
from datetime import datetime
from requests.exceptions import HTTPError
import pandas as pd
import psycopg2 as pg
from config import host, dbname, user, password, port
import pprint as pp
from http import HTTPStatus
import logging
from typing import Any
import os
from functools import cached_property


PATH = "/Users/danielsagher/Dropbox/Documents/projects/bls_api_project/"

national_series = pd.read_csv(
    PATH + "outputs/cleaning_op/national_series_dimension_cleaned.csv"
)
state_series = pd.read_csv(
    PATH + "outputs/cleaning_op/state_series_dimension_cleaned.csv"
)


class BlsApiCall:
    """
    Add Doc String
    """

    def __init__(self):
        self.query_count = 0
        self.message_df = pd.DataFrame(columns=["series_id", "year"])
        self._query_time = datetime.strftime(datetime.now(), "%m/%d/%Y, %H:%M:%S")
        self._query_day = datetime.strftime(datetime.now(), "%d")

    @property
    def query_day(self):
        return self._query_day

    @cached_property
    def first_query(self):
        return datetime.strftime(datetime.now(), "%d")

    #! Use a Run Count.txt file to count the amount of times a program has been run.

    @staticmethod
    def increment_query_count():

        count_file = "query_count.txt"

        # Check if the count file exists
        if not os.path.exists(count_file):
            print("not query count")
            with open(count_file, "w") as file:
                entry = "0, 0"
                file.write(str(entry))

        # Read the current count
        with open(count_file, "r") as file:
            result = file.read().strip()
            count, day = tuple(result.split(","))

        # Increment and write the updated count
        with open(count_file, "w") as file:
            query_day = datetime.strftime(datetime.now(), "%d")
            file.write(f"{int(count) + 1}, {query_day}")

    @property
    def get_query_count(self):

        count_file = "query_count.txt"
        if os.path.exists(count_file):
            with open(count_file, "r") as file:
                final_result = file.read()
                count, day = final_result.split(",")
                return count, day
        else:
            raise FileNotFoundError("Query count file does not exist.")

    def _query_limit(self):

        self.final_count, self.final_day = self.get_query_count

        if (
            int(self.final_count) > 500
            and int(self.final_day) - int(self.query_day) == 0
        ):
            raise Exception("Queries may not exceed 500 within a day.")

    def reset_query_limit(self):
        if int(self.final_day) - int(self.query_day) > 0:
            os.remove("query_count.txt")

    #! Something like this
    def bls_request(
        self, series: list, start_year: str, end_year: str) -> dict[str, Any]:  # fmt:skip
        """
        Rewrite docstring
        """
        URL_ENDPOINT = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        RETRIES = 3
        YEAR_LIMIT = 20
        SERIES_LIMIT = 50
        year_range = int(end_year) - int(start_year)

        if len(series) > SERIES_LIMIT:
            raise ValueError("Can only take up to 50 seriesID's per query.")
        elif year_range > YEAR_LIMIT:
            raise ValueError("Can only take in up to 20 years per query.")

        headers = {"Content-Type": "application/json"}
        payload = json.dumps(
            {
                "seriesid": series,
                "startyear": start_year,
                "endyear": end_year,
                "registrationKey": api_key.API_KEY,
            }
        )
        retry_codes = [
            HTTPStatus.INTERNAL_SERVER_ERROR,  # 500
            HTTPStatus.BAD_GATEWAY,  # 502
            HTTPStatus.SERVICE_UNAVAILABLE,  # 503
            HTTPStatus.GATEWAY_TIMEOUT,  # 504
        ]

        for attempt in range(1, RETRIES + 1):

            try:
                #! Just moved this up here and is not working.
                self._query_limit()
                response = rq.post(URL_ENDPOINT, data=payload, headers=headers)
                response_json = response.json()
                response_status = response_json["status"]

                if (response.status_code == HTTPStatus.OK and response_status == "REQUEST_SUCCEEDED"):  # fmt: skip

                    BlsApiCall.increment_query_count()
                    self.reset_query_limit()

                    return response_json
                else:
                    raise Exception(
                        f"API Error: {response_json.get('status', 'Unknown error')}"
                    )

            except HTTPError as e:
                if response.status_code in retry_codes:
                    time.sleep(2**attempt)
                    continue
                else:
                    raise HTTPError(f"HTTP error occurred: {e}")

            except Exception as e:
                time.sleep(2**attempt)  # Exponential backoff

        raise Exception("Failed to fetch data after multiple attempts.")

    def extract(
        self,
        series_df: pd.DataFrame,
        start_year: int = 2002,
        end_year: int = 2021,
    ) -> list:
        """ """
        series_id_lst = list(series_df["seriesID"])
        final_response_json_lst = []
        BATCH_SIZE = 50
        start_year = str(start_year)
        end_year = str(end_year)

        while series_id_lst:
            request_data = series_id_lst[:BATCH_SIZE]  # fmt: skip
            series_id_lst = series_id_lst[BATCH_SIZE:]  # fmt: skip

            print(f"Processessing batch of size: {len(request_data)}")
            print(request_data)

            result = self.bls_request(request_data, start_year, end_year)

            final_response_json_lst.append(
                result
            )  # Add the results to the final_response_json list
            time.sleep(0.25)

        return final_response_json_lst

    def set_message_ouput(self, message: str) -> None:
        """ """
        message_lst = []

        message_lst.append(message)

        series_id = message[29:-10]
        year = message[-4:]
        new_row = {"series_id": series_id, "year": year}
        #! self.message_df coming out empty
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
                self.set_message_ouput(message)

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
    result = call_engine.extract(national_series[:1], 2004, 2020)
    df = call_engine.transform(result)
    # print(call_engine.first_query)
