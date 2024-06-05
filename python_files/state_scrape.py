# Imports and constants

import requests as rq
from bs4 import BeautifulSoup as soup
import pandas as pd
import time
import datetime as dt

NOW = dt.datetime.now().strftime('%d-%b-%Y_%H:%M:%S')
PATH = '/Users/danielsagher/Dropbox/Documents/projects/bls_api_project/'
bls_state_url = 'https://data.bls.gov/cgi-bin/surveymost?sm'

def url_getter(url):

    # Get response from bls main state unemployment site
    print('Getting URL')
    bls_ro = rq.get(url)
    
    # Turn it into beautiful soup using BS4
    ro_soup = soup(bls_ro.text, 'html.parser')
    
    # Select only the tags containing links and names of states
    state_links = ro_soup.css.select('a[href^="https://data.bls.gov/cgi-bin/surveymost?"]')
    
    # List comprehension to make a list of links next to their respective states
    state_link_list = [(state_links[i].get('href'), state_links[i].get_text()) for i in range(len(state_links))][1:]

    return state_link_list




def state_scraper(state_link_list):

    '''
    This function scrapes the BLS State Employment page for all Series and SeriesID's 
    and deposits them into a Pandas DataFrame. 
    The result is then automatically saved to a CSV. 
    '''
    final_df = pd.DataFrame([])

    for link in range(len(state_link_list)):

        try:
            # Response from link
            print(f'Initializing "get" request for {state_link_list[link][1]} ')
            state_ro = rq.get(state_link_list[link][0]) 
            print(f'State Link for {state_link_list[link][1]} Acquired')

            # Soup
            state_soup = soup(state_ro.text, 'html.parser')

            # Find serials and survey names
            state_serial_and_id = state_soup.find_all('dt')[0].get_text()

            # Split into list
            state_serial_and_id = state_serial_and_id.split('\n')[1:-1]

            # Turn into DataFrame, split by '-'
            df = pd.DataFrame([item.split(' - ') for item in state_serial_and_id], columns=['series', 'seriesID'])

            # Take state name from series column and put it in its own column
            df['state'] = df['series'].str[0:(df['series'].str.find(',').astype(int)[0])]
            df['series'] = df['series'].str[(df['series'].str.find(', ').astype(int)[0]+2):]
            final_df['survey'] = 'CES'

            # Append to final DataFrame
            final_df = pd.concat([final_df, df], ignore_index= True)

            # Be nice to BLS
            print(f'{state_link_list[link][1]} data added to DataFrame')
            print('Sleeping for 2 seconds')

            time.sleep(2)
            
        except Exception as e:
            print(f"An error occurred while processing {state_link_list[link][1]}: {e}")

    final_df.to_csv(PATH + f'outputs/state_scrape_op/state_series_dimension_{NOW}.csv', index=False)
    
    return final_df
        


state_link_list = url_getter(bls_state_url)
state_scraper(state_link_list)