from api_bls import get_series_id, \
                    message_retriever, \
                    derated_call, \
                    dataframe_maker

import pandas as pd
import datetime as dt
import time


NOW = dt.datetime.now().strftime('%d-%b-%Y_%H:%M:%S')
PATH = '/Users/danielsagher/Dropbox/Documents/projects/bls_api_project/'

national_series = pd.read_csv(PATH + 'outputs/cleaning_op/national_series_dimension_cleaned.csv')
state_series = pd.read_csv(PATH + 'outputs/cleaning_op/state_series_dimension_cleaned.csv')


def main(series_input, name_of_file):

    '''
    This function takes in a list of seriesID's and a string 
    used to name the CSV file output. Input can be any # of seriesID's > than 1. 
    '''
    # Takes in input amd year in str
    # Defaults to 2001-2021 
    # List len has to be >1
    data_results = derated_call(series_input, '2002', '2015')


    # Pulls out no-data messages and stores in DF
    message_list = message_retriever(data_results)

    # Results of api caller run into a DF
    df = dataframe_maker(data_results)
    
    # Outputs files into csv's w/ date and time
    df.to_csv(f'{PATH}outputs/main_op/{name_of_file}_{NOW}.csv', index=False)
    message_list.to_csv(f'{PATH}outputs/main_op/{name_of_file}_msglst_{NOW}.csv', index=False)
    
    return df

# Run once for national_series, once for state_series
final_national_df  = main(national_series, 'national_results')
final_state_df = main(state_series, 'state_results')

