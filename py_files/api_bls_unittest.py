from unittest.mock import patch, Mock, create_autospec
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

        mocked_post.assert_called_with('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=payload,  headers=headers)
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


    @patch('api_bls.datetime.datetime')
    @patch('api_bls.requests.post')
    def test_bls_reset(self, mocked_post, mocked_date):

        if os.path.exists('query_count.txt'):
                os.remove('query_count.txt')

        mock_response = Mock()
        mock_response.status_code = HTTPStatus.OK
        mock_response.json.return_value = {"status": "REQUEST_SUCCEEDED"}
        mocked_post.return_value = mock_response

        mocked_date.strftime.return_value = '02'

        self.api_call.bls_request([1], 2000, 2005)
        self.api_call.bls_request([1], 2000, 2005)
        self.api_call.bls_request([1], 2000, 2005)
        self.api_call.bls_request([1], 2000, 2005)
        self.api_call.bls_request([1], 2000, 2005)

        mocked_date.strftime.return_value = '03'

        self.api_call.bls_request([1], 2000, 2005)

        with open('query_count.txt','r') as file:
            lines = []
            for line in file:
                lines.append(line)

            count, day = lines[0].split(',')
            count = int(count)
            day = int(day)

        assert len(lines) == 1
        assert count == 1
        assert day == 3
        




        

        


    
        
    


if __name__ == "__main__":
    unittest.main()