"""==========================================================

    Title:       <>
    Author:      <>
    Date:        <>
    Description:
        
    Special Concerns:

==========================================================="""

import csv
import datetime as dt
import tempfile
import unittest
from unittest.mock import Mock, patch

from main import main, interactive_user_input, read_file

class TestMain(unittest.TestCase):

    @patch('main.validate_path')
    @patch('main.validate_years')
    @patch('main.read_file')
    @patch('main.BlsApiCall')
    @patch('main.arg_parser')
    def test_all_main_args(self, mocked_parser,mocked_bls, mocked_read, mocked_years, mocked_path):
        args = mocked_parser()
        args.path = 'path/to/csv'
        args.series_type = 1
        args.start_year = 2000
        args.end_year = 2010
        mocked_years.return_value = True, None
        mocked_path.return_value = True, None
    
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
        args.path = 'path/to/csv'
        args.series_type = 1
        args.start_year = 2000
        args.end_year = False

        with self.assertRaises(ValueError) as e:
            main()
            
        self.assertEqual(str(e.exception), "Please Specify --path, --type, --start-year, and --end-year or nothing for interactive input.")
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
        args.path = False
        args.series_type = False
        args.start_year = False
        args.end_year = False
        mocked_user_input.return_value = {'path':'path/to/csv',
                                        'start_year': 2000,
                                        'end_year':2005,
                                        'series_type':1,
                                        'series_count':False}
        mocked_read.return_value = [{'col1':'value1', 'col2':'value2'}]
        mocked_years.return_value = True, None
        mocked_path.return_value = True, None

        main()
        mocked_bls.assert_called_once_with(2000, 2005, national_series=mocked_read.return_value, series_count=False)
        mocked_read.assert_called_once()
        mocked_years.assert_called_once()
        mocked_path.assert_called_once()
        mocked_user_input.assert_called_once()

    @patch('main.validate_years')
    @patch('main.validate_path')
    @patch('main.input')
    def test_interactive_user_input(self, mocked_input, mocked_path, mocked_years):
        responses = {"Enter CSV path: ": 'path/to/csv',
                    "Enter 1 for National Series, 2 for State Series: ": "1",
                    "Enter start year: ": "2000",
                    "Enter end year: ": "2005",
                    "Enter desired number of series from input (Press enter for all): ": "50"}
        mocked_path.return_value = True, None
        mocked_years.return_value = True, None
        mocked_input.side_effect = responses.get
        interactive_user_input()
        mocked_path.assert_called_once()
        mocked_years.assert_called_once()
        assert mocked_input.call_count == 5

    @patch('main.arg_parser')
    def test_csv_reader(self, mocked_args):
        fd, path = tempfile.mkstemp(suffix='csv', text=True)
        with open(path, 'w') as file:
            file.write('seriesID,series,state,survey,is_adjusted\n')
            file.write('123ABC,Always be Cool,MI,ABC,False\n')
            file.write('124ABC,Always be Cooler,MI,ABC,False\n')
        args = mocked_args()
        args.path = path
        args.series_type = 1
        args.start_year = 2000
        args.end_year = 2005
        read_file(args.path, args.series_type)

    @patch('main.validate_path')
    @patch('main.read_file')
    @patch('main.arg_parser')
    def test_csv_reader_error(self, mocked_args, mocked_read, mocked_path):
        args = mocked_args()
        args.path = 'path/to/csv'
        args.series_type = 1
        args.start_year = 2000
        args.end_year = 2005
        mocked_path.return_value = True, None

        mocked_read.side_effect = PermissionError
        with self.assertRaises(SystemExit):
            main()

        mocked_read.side_effect = ValueError
        with self.assertRaises(SystemExit):
            main()

        mocked_read.side_effect = csv.Error
        with self.assertRaises(SystemExit):
            main()

        mocked_read.side_effect = IOError
        with self.assertRaises(SystemExit):
            main()

        mocked_read.side_effect = UnicodeDecodeError
        with self.assertRaises(SystemExit):
            main()

        mocked_read.side_effect = Exception
        with self.assertRaises(SystemExit):
            main()
        
    @patch("main.arg_parser")
    @patch("main.validate_path")
    def test_path_exception(self, mocked_path, mocked_parser):
        args = mocked_parser()
        args.path = 'path/to/csv'
        args.series_type = 1
        args.start_year = 2000
        args.end_year = 2010
        mocked_path.return_value = False, "Path not found."
        with self.assertRaises(FileNotFoundError) as e:
            main()
        self.assertEqual(str(e.exception), "Path not found.")

    @patch("main.arg_parser")
    @patch("main.validate_path")
    def test_years_exception(self, mocked_path, mocked_parser):
        args = mocked_parser()
        args.path = 'path/to/csv'
        args.series_type = 1
        
        args.start_year = "two thousand twenty"
        args.end_year = 2010
        mocked_path.return_value = True, None
        with self.assertRaises(ValueError) as e:
            main()
        print(str(e.exception))
        self.assertEqual(str(e.exception), "Error: Years must be integers.")

        args.start_year = -2000
        args.end_year = 2010
        with self.assertRaises(ValueError) as e:
            main()
        self.assertEqual(str(e.exception), "Error: Years must be positive integers.")

        this_year = int(dt.datetime.strftime(dt.datetime.now(), "%Y"))
        args.start_year = 2000
        args.end_year = this_year + 10
        with self.assertRaises(ValueError) as e:
            main()
        self.assertEqual(str(e.exception), f"Error: End year cannot be in the future (current year: {this_year}).")

        args.start_year = 2010
        args.end_year = 200
        with self.assertRaises(ValueError) as e:
            main()
        self.assertEqual(str(e.exception), "Error: Start year must be before end year.")

        args.start_year = 2000
        args.end_year = 2025
        with self.assertRaises(ValueError) as e:
            main()
        self.assertEqual(str(e.exception), "Error: Year range cannot exceed 20 years due to API limitations.")

if __name__ == "__main__":
    unittest.main()