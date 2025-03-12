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

from unittest.mock import patch, Mock
import unittest
import os
from api_bls import BlsApiCall
from http import HTTPStatus
import json
from api_key import API_KEY
import pandas as pd
from requests.exceptions import HTTPError
import requests

class TestBlsApi(unittest.TestCase):

    query_count_file = "outputs/runtime_output/query_count.txt"

    def setUp(self):
        self.state_series = pd.DataFrame([{'survey':'ABC',
                                      'series':'I am series',
                                      'seriesID':'ABC123',
                                      'state': 'Michigan'}])
        self.national_series = pd.DataFrame([{'series': 'I am series',
                                              'seriesID':'ABC123',
                                              'survey': 'ABC'}])
        self.api_call = BlsApiCall(2000, 2005, state_series=self.state_series)
        return super().setUp()
    
    def tearDown(self):

        pass
        return super().tearDown()
    
    @patch('api_bls.logging.info')
    @patch('api_bls.requests.post')
    def test_bls_request(self, mocked_post, mocked_log):

        if os.path.exists(self.query_count_file):
            os.remove(self.query_count_file)

        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = {"status": "REQUEST_SUCCEEDED"}
        mocked_post.return_value = mock_response
        headers = {"Content-Type": "application/json"}
        payload = json.dumps(
            {
                "seriesid": ['ABC123'],
                "startyear": 2005,
                "endyear": 2007,
                "registrationKey": API_KEY,
            }
        )
        series_input = list(self.state_series['seriesID'])
        result = self.api_call.bls_request(series_input, 2005, 2007)

        mocked_post.assert_called_once_with('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=payload,  headers=headers)
        self.assertEqual(result, {"status": "REQUEST_SUCCEEDED"})
        assert mocked_log.call_count == 2

    @patch('api_bls.logging.critical')
    @patch('api_bls.requests.post')
    def test_bls_request_limit(self, mocked_post, mocked_log):
        if os.path.exists(self.query_count_file):
                os.remove(self.query_count_file)

        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = {"status": "REQUEST_SUCCEEDED"}
        mocked_post.return_value = mock_response
        
        for _ in range(1, 501):
            self.api_call._create_query_file()
            self.api_call._increment_query_count()
        
        with self.assertRaises(Exception) as e:
            self.api_call.bls_request([1], 2005, 2007)

        mocked_log.assert_called_once()
        mocked_post.assert_not_called()
        assert mocked_post.call_count <= 500
        self.assertEqual(str(e.exception), "Queries may not exceed 500 within a day.")

    @patch('api_bls.requests.post')
    def test_bls_year_limit(self, mocked_post):

        if os.path.exists(self.query_count_file):
                os.remove(self.query_count_file)
        
        with self.assertRaises(ValueError) as e:
            self.api_call.bls_request([1], 2000, 2025)
            mocked_post.assert_not_called()
        self.assertEqual(str(e.exception), "Can only take in up to 20 years per query.")
       
    @patch('api_bls.requests.post')
    def test_bls_id_limit(self, mocked_post):

        if os.path.exists(self.query_count_file):
                os.remove(self.query_count_file)
        
        mock_lst = [i for i in range(60)]

        with self.assertRaises(ValueError) as e:
            self.api_call.bls_request(mock_lst, 2000, 2005)
            mocked_post.assert_not_called()
        self.assertEqual(str(e.exception), "Can only take up to 50 seriesID's per query.")

    @patch('api_bls.logging.info')
    @patch('api_bls.logging.critical')
    @patch('api_bls.requests.post')
    def test_bls_bad_request(self, mocked_post, mocked_critical, mocked_info):

        mock_response = Mock()
        mock_response.status_code = HTTPStatus.BAD_REQUEST
        mocked_post.return_value = mock_response
        series_input = list(self.state_series['seriesID'])

        with self.assertRaises(HTTPError) as e:
            self.api_call.bls_request(series_input, 2000, 2005)
        mocked_critical.assert_called_once()
        mocked_post.assert_called_once()
        mocked_info.assert_called_once()
        self.assertEqual(str(e.exception), f"HTTP Error: {mocked_post().status_code}")

        
    @patch('api_bls.logging.critical')
    @patch('api_bls.logging.warning')
    @patch('api_bls.requests.post')
    def test_http_error_retry(self, mocked_post, mocked_warning, mocked_critical):
        
        mock_response = Mock()
        mock_response.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
        mocked_post.return_value = mock_response
        series_input = list(self.state_series['seriesID'])

        with self.assertRaises(Exception) as e1:
            with self.assertRaises(HTTPError):
                self.api_call.bls_request(series_input, 2000, 2002)
        
        self.assertEqual(str(e1.exception), f'API Error: {HTTPStatus.INTERNAL_SERVER_ERROR.value}')
        assert mocked_critical.call_count == 1
        assert mocked_warning.call_count == 3
        assert mocked_post.call_count == 3
     
    @patch('api_bls.logging.info')
    @patch('api_bls.datetime.datetime')
    @patch('api_bls.requests.post')
    def test_bls_reset(self, mocked_post, mocked_date, mocked_log):

        if os.path.exists(self.query_count_file):
                os.remove(self.query_count_file)

        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = {"status": "REQUEST_SUCCEEDED"}
        mocked_post.return_value = mock_response

        mocked_date.strftime.return_value = '02'

        for _ in range(5):
            self.api_call.bls_request([1], 2000, 2005)

        mocked_date.strftime.return_value = '03'

        self.api_call.bls_request([1], 2000, 2005)

        with open('outputs/runtime_output/query_count.txt','r') as file:
            lines = []
            for line in file:
                lines.append(line)

            count, day = lines[0].split(',')
            count = int(count)
            day = int(day)

        assert len(lines) == 1
        assert count == 1
        assert day == 3
        assert mocked_post.call_count == 6
        assert mocked_log.call_count == 12


    @patch('api_bls.BlsApiCall._log_message')
    @patch('api_bls.requests.post')
    def test_bls_log_function(self, mocked_post, mocked_log_function):                
        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = {'status': 'REQUEST_SUCCEEDED', 
                                           'responseTime': 225, 
                                           'message': ['No Data Available for Series 123456 Year: 1972'], 
                                           'Results': {
                                                    'series': [
                                                    {'seriesID': 'SMS01000000000000001', 
                                                     'data': []}]}}
        mocked_post.return_value = mock_response
        with self.assertRaises(Exception) as e:
            self.api_call.extract()
            self.api_call.transform()
        self.assertEqual(str(e.exception), 'DataFrame is Empty')

        mocked_log_function.assert_called_once()

    @patch('api_bls.logging.critical')
    @patch('api_bls.logging.warning')
    def test_response_error(self, mocked_warning, mocked_critical):
        
        config = {'side_effect': Exception()}
        patcher = patch('api_bls.requests.post', **config)
        mocked_requests = patcher.start()

        with self.assertRaises(Exception) as e1:
            self.api_call.bls_request(['ABC'], 2000, 2002)
        self.assertEqual(str(e1.exception), 'Response Status from API is not "REQUEST_SUCCEEDED"')
        assert mocked_warning.call_count == 0
        assert mocked_critical.call_count == 1
        assert mocked_requests.call_count == 1
        patcher.stop()
         
    def test_init_none(self):
        with self.assertRaises(Exception) as e:
            BlsApiCall()
            self.assertEqual(str(e.exception), 'Argument must only be one series list')

    def test_init_both(self):
        with self.assertRaises(Exception) as e:
            BlsApiCall(self.state_series, self.national_series)
            self.assertEqual(str(e.exception), 'Argument must only be one series list')
    
    def test_init_type(self):
        with self.assertRaises(Exception) as e:
             series = pd.Series(['ABC123', 'DEF456'])
             BlsApiCall(state_series=series)
             self.assertEqual(str(e.exception), 'BlsApiCall inputs must be Pandas DataFrame')
    
    def test_init_type_2(self):
        with self.assertRaises(Exception) as e:
             series = pd.Series(['ABC123', 'DEF456'])
             BlsApiCall(national_series=series)
             self.assertEqual(str(e.exception), 'BlsApiCall inputs must be Pandas DataFrame')
    
if __name__ == "__main__":
    unittest.main()