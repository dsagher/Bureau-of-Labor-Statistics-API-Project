# Bureau of Labor Statistics (BLS) API Data Project  

This repository contains the code and documentation for a project leveraging the **Bureau of Labor Statistics (BLS) API** to collect, process, and analyze labor data at both the national and state levels. The project is built in **Python** and uses **PostgreSQL** for database management and querying.  

## Files  
- `api_bls.py`  
- `api_bls_unittest.py`  
- `main.py`  
- `state_scrape.py`

## Features
**BlsApiCall Class**
Handles the extraction, transformation, and loading of BLS data.

*Built-in Protections*
- Daily query limit handling
- Incompatible input validation
- HTTP error handling
- Rate limit enforcement
- Year limit enforcement

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
## Response Structure
dict[str: str, str: int, str: list, str: dict[str: list[dict[str: list[dict[str:str]]]]]]

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