# Bureau of Labor Statistics (BLS) API Data Project

This repository contains the code and documentation for a project leveraging the Bureau of Labor Statistics (BLS) API to collect, process, and analyze labor data at both the national and state levels. The project is built in Python and uses PostgreSQL for database management and querying.

api_bls.py
api_bls_unittest.py
main.py
state_scrape.py

Example response_json:

    {'status': 'REQUEST_SUCCEEDED', 'responseTime': 225, 'message': [], 'Results': 
            {'series': [
            {'seriesID': 'SMS01000000000000001', 'data': [
            {'year': '2020', 'period': 'M12', 'periodName': 'December', 'value': '2022.5', 'footnotes': [{}]}, {}, {}, {}]}]}}
    - Structure: dict[str: str, str: int, str: list, str: dict[str: list[dict[str: list[dict[str:str]]]]]]

Example lst_of_queries:

                [
      start of new query -->{'status': 'REQUEST_SUCCEEDED', 'responseTime': 225, 'message': [], 'Results': 
                            {'series': 
list no longer than 50 IDs --> [
                            {'seriesID': 'SMS01000000000000001', 'data': [
                            {'year': '2020', 'period': 'M12', 'periodName': 'December', 
                            'value': '2022.5', 'footnotes': [{}]}, 
                            {next_periods/years}...]}
                                ]}},
    start of new query --> {'status': 'REQUEST_SUCCEEDED', 
                            'responseTime': 225,
                            'message': [],
                            'Results': 
                            {'series': [
                            {'seriesID': '123456789', 'data': [
                            {'year': '2020', 'period': 'M12', 'periodName': 'December', 
                            'value': '2022.5', 'footnotes': [{}]}, 
                            {next_periods/years}...]}]}}
                ]


contains BlsApiCall class with methods to extract, transform, and load data from the bureau of labor statistics. 

Includes protection against:
    - daily query limit
    - incompatible input
    - HTTP error handling
    - rate limit
    - year limit

Includes logging for 
