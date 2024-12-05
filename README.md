# Bureau of Labor Statistics (BLS) API Data Project

This repository contains the code and documentation for a project leveraging the Bureau of Labor Statistics (BLS) API to collect, process, and analyze labor data at both the national and state levels. The project is built in Python and uses PostgreSQL for database management, integrating data visualization tools for querying and reporting.

## Project Overview
### Goal
To transform raw labor data from the BLS API into a structured format suitable for analysis, enabling queries like:

(Example Query: Top 10 industries with the highest total employment in Georgia (November 2016).)

### Key Components

1. **Survey Table**  
Dimensions of various surveys and series.
    * Processed in Excel.
  
2. **Dimension Tables**  
Two separate tables for national and state data.
    * Processed in CSV format.
  
3. **Results Tables**  
Data tables for national and state-level results.
    * Extracted and processed using Python.
  
## Features

### Data Collection
* **API Call Functions**:
  
    * `get_series_id()`: Fetches series data from the BLS API.
    * `derated_call()`: Handles batch processing with rate limits, reindexing, and retries.
    
* **Web Scraping**:
  
    * `state_scraper()`: Automates the extraction of state-level series IDs and titles.
    
### Data Transformation

* **Message Retrieval**:
  
    * `message_retriever()`: Identifies and processes error messages returned by the API (e.g., missing years).
* **DataFrame Creation**:
  
    * `dataframe_maker()`: Converts JSON API responses into structured Pandas DataFrames.
      
### Data Storage
* PostgreSQL is used to store cleaned and transformed data:
    * **National Results**: ~38,955 rows.
    * **State Results**: ~323,628 rows.
    
### Example Query
Find industries with the highest employment in Georgia (November 2016):

```sql
SELECT ss.state,
       ss.series,
       st.year,
       st.period_name,
       st.value
FROM public.state_results st
JOIN state_series ss USING(seriesID)
WHERE state = 'Georgia'
  AND year = 2016
  AND period_name = 'November'
  AND is_adjusted = TRUE
  AND series != 'Total Nonfarm'
ORDER BY st.value DESC
LIMIT 10;
``` 


## Technologies Used
* **Python**:
    * Libraries: `pandas`, `requests`, `json`, `time`, `beautifulsoup4`
* **PostgreSQL**: Database for structured storage and analysis.
* **Excel**: Manual data processing and organization.
* **API**: Integration with the BLS API.
