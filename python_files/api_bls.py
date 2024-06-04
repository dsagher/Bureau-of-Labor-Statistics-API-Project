# Define API call

import pandas as pd
import requests as rq
from requests.exceptions import HTTPError
import json
import api_key
import time
from IPython.display import clear_output

URL_ENDPOINT = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'


def get_series_id(series, start_year, end_year):

    '''
    This is the main API call. This gets run within the derated_call() function.
    Will error if more less than 2 seriesID's are input, which would be caught in derated_call().
    '''

    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({"seriesid": series, "startyear": start_year, "endyear": end_year, "registrationKey": api_key.API_KEY})
    
    try:
        ro = rq.post(URL_ENDPOINT, data=payload, headers=headers)
        ro.raise_for_status()
        result = ro.json()

    except HTTPError as e:
         
         print(f'HTTP Error: {e}')
         return None, 'HTTP Error'
        
    except Exception as e:

        print(f'An error occurred: {e}')
        return None, 'Error'

    return result, 'Done'





def message_retriever(data_results):

    '''
    When there is no data for a specific year and seriesID, a message is returned. 
    This function compiles a new DataFrame with those messages,
    with seperate columns for |message|serialID|year|
    '''

    message_list = []
    for call in data_results:

        message_list.extend(call['message'])

    df = pd.DataFrame(message_list, columns = ['message'])
    df['serialID'] = df['message'].apply(lambda x: x[29:-10])
    df['year'] = df['message'].apply(lambda x: x[-4:])

    return df





def derated_call(lst, start_year = '2002', end_year = '2021'):

    ''' 
    Feeds batches of 50 seriesID's into get_series_id(). It sleeps for 5 seconds in between batches. 
    '''

    lst = list(lst['seriesID'])
    final = [] 
    batch_size = 50 
    start_year = str(start_year)
    end_year = str(end_year)

    try:
        if len(lst) < 2: # Error handling for input error 
            raise Exception
        
        while lst: 
            data = lst[:batch_size] # Get the first 50 items of the batch
            lst = lst[batch_size:] # Remove processed items from list

            print(f'Processessing batch of size: {len(data)}')
            print(data)

            result, status = get_series_id(data, start_year, end_year)
            
            if status == 'Done': 
                print('API call successful')
                final.append(result) # Add the results to the final list
                print('Sleeping for 5 seconds') # Call API
            elif status == 'HTTP Error':
                print('HTTP Error occurred during API call')
            else:
                print('Error occurred during API call')
            
            time.sleep(5) # Sleep
            clear_output()

    except Exception as e:

        print(e, "Input list must be at include at least 2 seriesID's")

    return final








def dataframe_maker(data_results):
    
    '''
    Creates Pandas DataFrames from JSON response.
    '''

    final_df = pd.DataFrame([])
    
    print('Creating DataFrame...')
    for call in data_results: 
        
        for series in call['Results']['series']:
            seriesID = series['seriesID']
            
            for data_point in series['data']:
                data_dict = {
                    'seriesID': seriesID,
                    'year': data_point['year'],
                    'period': data_point['period'],
                    'period_name': data_point['periodName'],
                    'value': data_point['value'],
                    'footnotes': data_point['footnotes'] if not '[{}]' in data_point else None
                }
                
                df = pd.DataFrame([data_dict])
                final_df = pd.concat([final_df, df], ignore_index=True)
    
    print('DataFrame Created')
    return final_df



        