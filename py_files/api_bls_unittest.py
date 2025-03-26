"""==========================================================================================
    File:        test_api_bls.py
    Author:      Dan Sagher
    Date:        3/11/25
    Description:
        This module contains unit tests for the BlsApiCall class that extracts, transforms,
        and loads data from the Bureau of Labor Statistics (BLS) API.

    Dependencies:

        External:
        - unittest
        - unittest.mock
        - pandas
        - http.HTTPStatus
        - json
        - os
        - requests.exceptions

        Internal:
        - api_bls
        - api_key

    Special Concerns: 
        - Tests require a mock BLS API environment
        - Some tests modify the query_count_file and need to clean up afterward
        - All API calls are mocked to avoid hitting real API endpoints
=========================================================================================="""

import json
import os
import unittest
from http import HTTPStatus
from unittest.mock import Mock, patch

import pandas as pd
from requests.exceptions import HTTPError

from api_bls import BlsApiCall

class TestBlsApi(unittest.TestCase):
    """
    Test suite for BlsApiCall class that handles BLS API interactions.
    
    These tests cover initialization, API request handling, error conditions,
    rate limiting, and data transformation functionality.
    """

    query_count_file = "outputs/runtime_output/query_count.txt"

    def setUp(self):
        """
        Set up test fixtures before each test method runs.
        
        Creates sample state and national series DataFrames and initializes
        a BlsApiCall instance for testing.
        """
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
        """
        Currently does nothing.
        """
        pass
        return super().tearDown()
    
    @patch('api_bls.logging.info')
    @patch('api_bls.requests.post')
    def test_bls_request(self, mocked_post, mocked_log):
        """
        Test successful API request functionality.
        
        Verifies:
        - The correct URL endpoint is used
        - Proper parameters are passed to the API
        - Successful response is handled correctly
        - Logging occurs as expected
        """
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
                "registrationKey": os.getenv("BLS_API_KEY"),
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
        """
        Test API request limit enforcement.
        
        Verifies an exception is raised when exceeding the 500 daily request limit.
        Confirms logging and that no actual API call is made when limit is reached.
        """
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
        """
        Test year range limit enforcement.
        
        Verifies that requests spanning more than 20 years are rejected
        with an appropriate ValueError exception.
        """
        if os.path.exists(self.query_count_file):
                os.remove(self.query_count_file)
        
        with self.assertRaises(ValueError) as e:
            self.api_call.bls_request([1], 2000, 2025)
            mocked_post.assert_not_called()
        self.assertEqual(str(e.exception), "Can only take in up to 20 years per query.")
       
    @patch('api_bls.requests.post')
    def test_bls_id_limit(self, mocked_post):
        """
        Test series ID limit enforcement.
        
        Verifies that requests containing more than 50 series IDs are rejected
        with an appropriate ValueError exception.
        """
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
        """
        Test handling of bad HTTP responses.
        
        Verifies that non-200 HTTP responses result in appropriate exceptions
        and that proper logging occurs.
        """
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
        """
        Test retry mechanism for server errors.
        
        Verifies that server errors (5xx) trigger the retry mechanism
        and that the appropriate number of retries occur before failing.
        """    
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
        """
        Test query counter reset on date change.
        
        Verifies that the query counter resets when the date changes,
        ensuring that the daily limit is properly managed.
        """
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
        """
        Test handling of API message logging.
        
        Verifies that messages in the API response (like missing data warnings)
        are properly logged by the _log_message method.
        """              
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

        self.api_call.extract()
        self.api_call.transform()
        mocked_log_function.assert_called_once()

    @patch('api_bls.logging.critical')
    @patch('api_bls.logging.warning')
    def test_response_error(self, mocked_warning, mocked_critical):
        """
        Test handling of non-success API responses.
        
        Verifies proper exception raising and logging when the API returns
        a response with a status other than "REQUEST_SUCCEEDED".
        """
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
        """
        Test class initialization with no series data.
        
        Verifies that an exception is raised when neither state_series
        nor national_series is provided to the constructor.
        """
        with self.assertRaises(Exception) as e:
            BlsApiCall()
            self.assertEqual(str(e.exception), 'Argument must only be one series list')

    def test_init_both(self):
        """
        Test class initialization with both series types.
        
        Verifies that an exception is raised when both state_series
        and national_series are provided to the constructor.
        """
        with self.assertRaises(Exception) as e:
            BlsApiCall(self.state_series, self.national_series)
            self.assertEqual(str(e.exception), 'Argument must only be one series list')
    
    
if __name__ == "__main__":
    unittest.main()