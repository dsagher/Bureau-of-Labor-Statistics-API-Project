
Instructions for configuring and running BLS API pipeline.



# Setting up a Virtual Environment (Recommended):
```bash
# Create venv
python -m venv path/to/your/project/venv

# Export environmental variables
echo 'export BLS_API_KEY=""' >> path/to/your/project/venv/bin/activate
echo 'export DRIVER=""' >> path/to/your/project/venv/bin/activate
echo 'export HOST=""' >> path/to/your/project/venv/bin/activate
echo 'export DATABASE=""' >> path/to/your/project/venv/bin/activate
echo 'export USERNAME=""' >> path/to/your/project/venv/bin/activate
echo 'export PASSWORD=""' >> path/to/your/project/venv/bin/activate
echo 'export PORT=""' >> path/to/your/project/venv/bin/activate

# Activate venv
source path/to/your/project/venv/bin/activate

# Install requirements
pip install -r path/to/your/project/requirements.txt
```

# For global environment use:
```bash
# Install requirements
pip install -r requirements.txt

# Export environmental variables
export BLS_API_KEY=""
export DRIVER=""
export HOST=""
export DATABASE=""
export USERNAME=""
export PASSWORD=""
export PORT=""
```

```bash
# For interactive input
cd path/to/your/project/scripts
python main.py 
```

```bash
# For command line input example:
cd path/to/your/project/scripts
python main.py --path path/to/input/state_series_dimension.csv --series-type 2 --start-year 2000 --end-year 2005
```

More options:
```bash
-n, --series-count: Enter number of seriesIDs to input.
-p, --ping: Enter number of pings to send to the BLS API. Will exit program after execution.
-t, --traceroute: Check routing to the BLS API. Will exit program after execution.
-v, --verbose: Include more information to logging output (Set level to debug).
-o, --output: Flag to generate CSV output of results
-s, --silence: Turn off logging to console and file. Takes precedent over --output.
```