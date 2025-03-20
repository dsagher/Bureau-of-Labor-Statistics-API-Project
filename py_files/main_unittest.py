from unittest.mock import patch, Mock
import unittest
from main import main, interactive_user_input
import subprocess
import os
import sys

class TestMain(unittest.TestCase):

    @patch('main.validate_path')
    @patch('main.validate_years')
    @patch('main.read_file')
    @patch('main.BlsApiCall')
    @patch('main.arg_parser')
    def test_all_main_args(self, mocked_parser,mocked_bls, mocked_read, mocked_years, mocked_path):
        args = mocked_parser()
        args.csv_path = 'Hello'
        args.series_type = 1
        args.start_year = 2000
        args.end_year = 2010
        mocked_years.return_value = True
        mocked_path.return_value = True
    
        main()
        mocked_years.assert_called_once()
        mocked_path.assert_called_once()
        mocked_read.assert_called_once()
        mocked_bls.assert_called_once()

    @patch('main.validate_path')
    @patch('main.validate_years')
    @patch('main.read_file')
    @patch('main.BlsApiCall')
    @patch('main.arg_parser')
    def test_main_args_missing(self, mocked_parser,mocked_bls, mocked_read, mocked_years, mocked_path):
        args = mocked_parser()
        args.csv_path = 'Hello'
        args.series_type = 1
        args.start_year = 2000
        args.end_year = False

        with self.assertRaises(ValueError) as e:
            main()
            
        self.assertEqual(str(e.exception), "Please Specify --csv-path, --type, --start-year, and --end-year or nothing for interactive input.")
        mocked_years.assert_not_called()
        mocked_path.assert_not_called()
        mocked_read.assert_not_called()
        mocked_bls.assert_not_called()

    @patch('main.interactive_user_input')
    @patch('main.validate_path')
    @patch('main.validate_years')
    @patch('main.read_file')
    @patch('main.BlsApiCall')
    @patch('main.arg_parser')
    def test_run_interactive_user_input(self, mocked_parser,mocked_bls, mocked_read, mocked_years, mocked_path, mocked_user_input):
        args = mocked_parser()
        args.csv_path = False
        args.series_type = False
        args.start_year = False
        args.end_year = False
        mocked_user_input.return_value = {'path':'path/to/csv',
                                        'start_year': 2000,
                                        'end_year':2005,
                                        'series_type':1,
                                        'number_of_series':False}
        mocked_read.return_value = [{'col1':'value1', 'col2':'value2'}]

        main()
        mocked_bls.assert_called_once_with(2000, 2005, national_series=mocked_read.return_value, number_of_series=False)
        mocked_read.assert_called_once()
        mocked_years.assert_called_once()
        mocked_path.assert_called_once()
        mocked_user_input.assert_called_once()

    @patch('main.input')
    def test_interactive_user_input(self, mocked_input):
        mocked_input.return_value = 3
        with self.assertRaises(ValueError) as e:
            interactive_user_input()
        self.assertEqual(str(e.exception), 'Series type must be 1 for National Series or 2 for State Series.')

        


if __name__ == "__main__":
    unittest.main()