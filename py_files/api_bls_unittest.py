from unittest.mock import patch, Mock
import unittest
import os
from api_bls import BlsApiCall
from http import HTTPStatus
import json
from api_key import API_KEY
from requests.exceptions import HTTPError


class TestBlsApi(unittest.TestCase):

    def setUp(self):
            
        self.api_call = BlsApiCall()
       
        return super().setUp()
    
    def tearDown(self):

        pass
        return super().tearDown()

    @patch('api_bls.requests.post')
    def test_bls_request(self, mocked_post):
        if os.path.exists('query_count.txt'):
            os.remove('query_count.txt')
        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = {"status": "REQUEST_SUCCEEDED"}
        mocked_post.return_value = mock_response

        result = self.api_call.bls_request([1], 2005, 2007)

        headers = {"Content-Type": "application/json"}
        payload = json.dumps(
            {
                "seriesid": [1],
                "startyear": 2005,
                "endyear": 2007,
                "registrationKey": API_KEY,
            }
        )

        mocked_post.assert_called_with('https://api.bls.gov/publicAPI/v2/timeseries/data/',
                                       data = payload, 
                                       headers = headers)
        self.assertEqual(result, {"status": "REQUEST_SUCCEEDED"})
        

    @patch('api_bls.requests.post')
    def test_bls_request_limit(self, mocked_post):
        if os.path.exists('query_count.txt'):
                os.remove('query_count.txt')
        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = {"status": "REQUEST_SUCCEEDED"}
        mocked_post.return_value = mock_response
        
        for i in range(1, 502):
            self.api_call.bls_request([1], 2005, 2007)

        with self.assertRaises(Exception) as e:
            self.api_call.bls_request([1], 2005, 2007)
            self.assertEqual(str(e.exception), "Queries may not exceed 500 within a day.")
            self.assertFalse(os.path.exists('query_count.txt'))
        

    @patch('api_bls.requests.post')
    def test_bls_year_limit(self, mocked_post):

        if os.path.exists('query_count.txt'):
                os.remove('query_count.txt')

        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = {"status": "REQUEST_SUCCEEDED"}
        mocked_post.return_value = mock_response
        
        with self.assertRaises(ValueError) as e:
            self.api_call.bls_request([1], 2000, 2025)
            self.assertEqual(str(e.exception), "Can only take in up to 20 years per query.")
        

    @patch('api_bls.requests.post')
    def test_bls_id_limit(self, mocked_post):

        if os.path.exists('query_count.txt'):
                os.remove('query_count.txt')

        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = {"status": "REQUEST_SUCCEEDED"}
        mocked_post.return_value = mock_response
        
        mock_lst = [i for i in range(60)]

        with self.assertRaises(ValueError) as e:
            self.api_call.bls_request(mock_lst, 2000, 2005)

            self.assertEqual(str(e.exception), "Can only take up to 50 seriesID's per query.")

    @patch('api_bls.requests.post')
    def test_bls_id_limit(self, mocked_post):

        if os.path.exists('query_count.txt'):
                os.remove('query_count.txt')

        mock_response = Mock()
        mock_response.status_code = HTTPStatus.BAD_REQUEST
        mock_response.json.return_value = {"status": "BAD"}
        mocked_post.return_value = mock_response
    
        with self.assertRaises(Exception) as e:
            self.api_call.bls_request([1], 2000, 2005)
            self.assertEqual(str(e.exception), f"API Error: {mocked_post().json()['status']}, Code: {mocked_post().status_code}")

    @patch('api_bls.BlsApiCall.reset_query_limit')
    @patch('api_bls.BlsApiCall.increment_query_count')
    @patch('api_bls.BlsApiCall.raise_for_query_limit')
    @patch('api_bls.BlsApiCall.create_first_query')
    @patch('api_bls.requests.post')
    def test_bls_reset(self, mocked_post, mocked_first_query, mocked_raise_for_query_limit, mocked_increment_query_count, mocked_reset):

        if os.path.exists('query_count.txt'):
                os.remove('query_count.txt')

        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = {"status": "REQUEST_SUCCEEDED"}

        # mocked_first_query.first_query_day = 2
        # mocked_first_query.count = 1
        # mocked_first_query.just_created = False
        # with open('query_count.txt', "w") as file:
        #     entry = f"{mocked_first_query.count}, {mocked_first_query.first_query_day}\n"
        #     file.write(str(entry))
        #     mocked_first_query.just_created = True
        
        # mocked_increment_query_count.just_created = True

        # mocked_raise_for_query_limit.current_query_day = 1
        # mocked_raise_for_query_limit.first_query_day = 2

        mocked_reset.current_query_day = 2
        mocked_reset.first_query_day = 1

        self.api_call.bls_request([1], 2000, 2005)
        self.assertFalse(os.path.exists('query_count.txt'))
        # with self.assertRaises(Exception) as e:
        


    
        
    


if __name__ == "__main__":
    unittest.main()