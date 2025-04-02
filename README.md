# Bureau of Labor Statistics (BLS) API Data Project  

This repository contains the code and documentation for a project leveraging the **Bureau of Labor Statistics (BLS) API** to collect, process, and analyze labor data at both the national and state levels. The project is built in **Python** and uses **PostgreSQL** for database management and querying.

This pipeline can be automated using `cron`, but given the infrequent updates, manual execution may be sufficient. Since the data is independent of other sources, complex orchestration tools are unnecessary.

## Files  
- `api_bls.py`  
    - Contains code for extracting, transforming, and loading data
      from the BLS API, as well as handling rate and daily query limits.
- `api_bls_unittest.py`  
    - Contains unit testing for the api_bls module.
- `main.py`
    - Contains the main logic processing user input, validating input,
      and calling ETL processes.
- `main.unittest.py`
    - Contains unit testing for the main module.

## File Structure
```
.
└── Bls_Api_Project/
    ├── scripts/
    │   ├── main.py
    │   ├── main_unittest.py
    │   ├── api_bls.py
    │   └── api_bls_unittest.py
    ├── inputs/
    │   ├── national_series.csv
    │   └── state_series.csv
    ├── outputs/
    │   ├── main.log
    │   └── query_count.txt
    ├── requirements.txt
    ├── instructions.md
    ├── README.md
    └── .gitignore
```
## Inputs
- `national_series_dimension.csv`
    - CSV input file containing series, seriesID, and survey
Ex:
    ```
    series,seriesID,survey
    Avg hrs per day Watching TV,TUU10101AA01014236,ATUS
    ```

- `state_series_dimension.csv`
    - CSV input file containing series, seriesID, state, survey
Ex: 
    ```
    series, seriesID, state, survey
    Total Nonfarm, Seasonally adjusted,SMS01000000000000001, Alabama, CES
    ```

## Outputs
- `main.log`
    - Stores runtime logs (errors, execution details, timestamps).
- `query_count.txt`
    - Tracks query count to ensure compliance with daily rate limit of 500 queries.

*Built-in Protections*
- Daily query limit handling (prevents exceeding 500 requests).
- Invalid input detection (ensures valid queries before execution).
- HTTP error handling (retries or logs failures).
- Rate limit enforcement (throttles requests to prevent bans).
- Year limit enforcement (restricts queries to valid year ranges).
- Flexible input options:
- Interactive mode (python main.py).
- Command-line input (python main.py --path ...).

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