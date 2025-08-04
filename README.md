# Hikeable
A simple data pipeline and dashboard to check the weather and hike-ability of a city

The data pipeline portion is a script that takes a relative path to a JSON config file as an argument and will fetch data from an API according to the configs in the JSON file.
The script then processes the API response according to configs, and saves the desired data to a parquet file.
If required, the script will go about loading the data into a data warehouse for use by a BI Tool, currently only BigQuery is supported.

### Setup

Run the setup script to configure your environment:

```bash
./setup.sh
```

### Usage

Activate the virtual environment if not already activated
```bash
source .venv/bin/activate
```
