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

from api_bls import BlsApiCall
import pandas as pd
import datetime as dt
import os


NOW = dt.datetime.now().strftime("%d-%b-%Y_%H:%M:%S")
#! Use OS Module
PATH = "/Users/danielsagher/Dropbox/Documents/projects/bls_api_project/"

#! Could try to webscrape national series too
national_series = pd.read_csv(
    PATH + "outputs/cleaning_op/national_series_dimension_cleaned.csv"
)
state_series = pd.read_csv(
    PATH + "outputs/cleaning_op/state_series_dimension_cleaned.csv"
)

api_engine = BlsApiCall()


def main(series_input: None, name_of_file: None) -> None:
    """

    :params:
    :returns:
    """
    data_results = api_engine.extract(series_input[0:10], "2002", "2015")

    df = api_engine.transform(data_results)

    df.to_csv(f"{PATH}outputs/main_op/{name_of_file}_{NOW}.csv", index=False)

    return df


if __name__ == "__main__":
    final_national_df = main(national_series, "national_results")
    final_state_df = main(state_series, "state_results")
