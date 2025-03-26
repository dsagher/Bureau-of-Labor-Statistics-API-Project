# Bureau of Labor Statistics (BLS) API Data Project  

This repository contains the code and documentation for a project leveraging the **Bureau of Labor Statistics (BLS) API** to collect, process, and analyze labor data at both the national and state levels. The project is built in **Python** and uses **PostgreSQL** for database management and querying.  

## Files  
- `api_bls.py`  
    - Contains code for extracting, transforming, and loading data from
      from the BLS API, as well as handling rate and daily query limits.
- `api_bls_unittest.py`  
    - Contains unit testing for the api_bls module.
- `main.py`
    - Contains the main logic processing user input, validating input,
      and calling ETL processes.
- `main.unittest.py`
    - Contains unit testing for the main module.

## Authentication
    - API obtained from BLS website must be stored in an environmental
      variable name BLS_API_KEY.
      
## Inputs
- `config.json`
    - JSON specification for database connection info.
    - Must be contained in projectFolder/inputs/config.json
        - ex:{
              "driver": "driver",
              "host": "host",
              "database": "database",
              "username": "username",
              "password": "password",
              "port": 1234
              }
- `national_series_dimension.csv`
    - CSV input file containing series, seriesID, and survey
        - ex: series,seriesID,survey\n
              Avg hrs per day Watching TV,TUU10101AA01014236, ATUS\n
- `state_series_dimension.csv`
    - CSV input file containing series, seriesID, state, survey
        - ex: series, seriesID, state, survey\n
            Total Nonfarm, Seasonally adjusted,SMS01000000000000001, Alabama, CES\n

## Outputs
- `main.log`
    - Contains runtime information.
- `query_count.txt`
    - Tracks query count to ensure compliance with daily rate limit of 500 queries.

*Built-in Protections*
- Daily query limit handling
- Incompatible input validation
- HTTP error handling
- Rate limit enforcement
- Year limit enforcement
- Interactive input or command line input

## Example API Response  
```json
{
    "status": "REQUEST_SUCCEEDED",
    "responseTime": 225,
    "message": [],
    "Results": {
        "series": [
            {
                "seriesID": "SMS01000000000000001",
                "data": [
                    {"year": "2020", "period": "M12", "periodName": "December", "value": "2022.5", "footnotes": [{}]}
                ]
            }
        ]
    }
}
```
*Response Structure:* dict[str: str, str: int, str: list, str: dict[str: list[dict[str: list[dict[str:str]]]]]]

## Example List of Queries:
```json
[
    {
        "status": "REQUEST_SUCCEEDED",
        "responseTime": 225,
        "message": [],
        "Results": {
            "series": [
                {
                    "seriesID": "SMS01000000000000001",
                    "data": [
                        {"year": "2020", "period": "M12", "periodName": "December", "value": "2022.5", "footnotes": [{}]}
                    ]
                }
            ]
        }
    },
    {
        "status": "REQUEST_SUCCEEDED",
        "responseTime": 225,
        "message": [],
        "Results": {
            "series": [
                {
                    "seriesID": "123456789",
                    "data": [
                        {"year": "2020", "period": "M12", "periodName": "December", "value": "2022.5", "footnotes": [{}]}
                    ]
                }
            ]
        }
    }
]
```