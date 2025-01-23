import psycopg2 as ps
import pandas as pd
from config import host, dbname, user, password, port

PATH = '/Users/danielsagher/Dropbox/Documents/projects/bls_api_project/'

conn = ps.connect(host=host, dbname=dbname, user=user,
                  password=password, port=port)

cur = conn.cursor()


cur.execute('''
            --sql
            CREATE TABLE IF NOT EXISTS state_series (
            seriesID VARCHAR PRIMARY KEY,
            series VARCHAR,
            state VARCHAR,
            survey VARCHAR,
            is_adjusted BOOLEAN
            );

            --sql
            CREATE TABLE IF NOT EXISTS national_series (
            seriesID VARCHAR PRIMARY KEY,
            series VARCHAR,
            survey VARCHAR,
            is_adjusted BOOLEAN
            );

            --sql
            CREATE TABLE IF NOT EXISTS state_results (
            seriesID VARCHAR,
            year INT,
            period VARCHAR,
            period_name VARCHAR,
            value FLOAT,
            footnotes VARCHAR
            );

            --sql
            CREATE TABLE IF NOT EXISTS national_results (
            seriesID VARCHAR,
            year INT,
            period VARCHAR,
            period_name VARCHAR,
            value FLOAT,
            footnotes VARCHAR
            );

            --sql
            CREATE TABLE IF NOT EXISTS survey_table (
            survey VARCHAR, 
            survey_name VARCHAR
            );
            
            ''')

cur.execute(f'''
            --sql
            COPY national_series
            FROM '{PATH}/outputs/cleaning_op/national_series_dimension_cleaned.csv' DELIMITER ',' CSV HEADER;

            --sql
            COPY state_series
            FROM '{PATH}/outputs/cleaning_op/state_series_dimension_cleaned.csv' DELIMITER ',' CSV HEADER;

            --sql
            COPY national_results
            FROM '{PATH}/outputs/cleaning_op/national_results_cleaned.csv' DELIMITER ',' CSV HEADER;

            --sql
            COPY state_results
            FROM '{PATH}/outputs/cleaning_op/state_results_cleaned.csv'  DELIMITER ',' CSV HEADER;

            --sql
            COPY survey_table
            FROM '{PATH}/outputs/excel_op/survey_table.csv' DELIMITER ',' CSV HEADER;
''')    
            

conn.commit()
cur.close()
conn.close()