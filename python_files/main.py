from api_bls import get_series_id, \
                    message_retriever, \
                    derated_call, \
                    dataframe_maker

import pandas as pd
import datetime as dt
from IPython.display import clear_output
import time


NOW = dt.datetime.now().strftime('%d-%b-%Y_%H:%M:%S')
PATH = '/Users/danielsagher/Dropbox/Documents/projects/bls_api_project/'

national_series = pd.read_csv(PATH + 'outputs/cleaning_op/national_series_dimension_cleaned.csv')
state_series = pd.read_csv(PATH + 'outputs/cleaning_op/state_series_dimension_cleaned.csv')


def main(series_input, name_of_file):

    '''
    This function takes in a list of seriesID's and a string which will 
    used to name the CSV file output. Input can be any number of seriesID's 
    greater than 1. 
    '''
    
    data_results = derated_call(series_input, '2002', '2021')

    message_list = message_retriever(data_results)

    df = dataframe_maker(data_results)
    
    df.to_csv(PATH + f'outputs/main_op/{name_of_file}_{NOW}.csv', index=False)
    message_list.to_csv(PATH + f'outputs/main_op/{name_of_file}_msglst_{NOW}.csv', index=False)

    
    time.sleep(1)
    clear_output()
    
    return df


final_national_df  = main(national_series, 'national_results')
final_state_df = main(state_series, 'state_results')

