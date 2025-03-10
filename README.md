# Bureau of Labor Statistics (BLS) API Data Project

This repository contains the code and documentation for a project leveraging the Bureau of Labor Statistics (BLS) API to collect, process, and analyze labor data at both the national and state levels. The project is built in Python and uses PostgreSQL for database management and querying.

api_bls.py

contains BlsApiCall class with methods to extract, transform, and load data from the bureau of labor statistics. 

Includes protection against:
    - daily query limit
    - incompatible input
    - HTTP error handling
    - rate limit
    - year limit

Includes logging for 
