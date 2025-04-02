
Instructions for configuring and running BLS API pipeline.

- Linux/macOS

```bash
export BLS_API_KEY=""
export DRIVER=""
export HOST=""
export DATABASE=""
export USERNAME=""
export PASSWORD=""
export PORT=""
```

for virtual environment:
```bash
python -m venv path/to/your/project/venv
echo 'export BLS_API_KEY=""' >> path/to/your/project/venv/bin/activat
echo 'export DRIVER=""' >> path/to/your/project/venv/bin/activate
echo 'export HOST=""' >> path/to/your/project/venv/bin/activate
echo 'export DATABASE=""' >> path/to/your/project/venv/bin/activate
echo 'export USERNAME=""' >> path/to/your/project/venv/bin/activate
echo 'export PASSWORD=""' >> path/to/your/project/venv/bin/activate
echo 'export PORT=""' >> path/to/your/project/venv/bin/activate
```

```bash
# For interactive input
cd path/to/your/project/scripts
python main.py 
```

```bash
# For command line input
cd path/to/your/project/scripts
python main.py --path path/to/series_input --series-type n --start-year n --end-year n
```

```bash
# For more options
python main.py -h


