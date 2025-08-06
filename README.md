# Hikeable
A generic data pipeline paired with a dashboard to check the weather and "hikeability" of a city.

The data pipeline portion is a script that takes a relative path to a JSON config file as an argument and will fetch data from a source according to the configs in the JSON file.
The script then writes the data to the specified destination.

The script can handle deltas either based only on primary key, or all columns.

To update the data in the destination, simply run the pipeline again using the same command.

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

Run the pipeline by passing the relative path to a config file
```bash
python pipeline.py <path_to_config_file>
```

Example:
```bash
python pipeline.py "configs/San Francisco.json"
```

### Design Choices

1. Python: Chosen primarily because it has so many libraries that make building integrations and working with data very easy, but also because of my familiarity with the language.
2. GCP: Chosen for its broad set of data orientated services, including both a data warehouse (BigQuery) and BI Tool (Looker Studio) making it a good fit for this project. Again, my familiarity with the platform also played a role.
3. Config based pipelines: Makes it very easy to build and run pipelines without writing code. In the future, a web front end or even an AI could create the config files based on inputs or requirements, opening the possibilities for rapid pipeline creation.
4. ELT was chosen for the following reasons:
    1. **Quicker to Build**: Performing transformations within the pipelines requires serious amounts of rigor within the code to ensure high data quality and integrity post transformation.
    2. **Easier to Maintain**: ELT pipelines are not as feature rich, sometimes requiring the data to be processed after being loaded to it's destination, but the pipeline is more robust and maintainable as it is focused on doing one thing well (moving data).
    3. **Better Scalability**: ELT is usually preferred when the volume and velocity of the data is high, and although the volumes and velocity of this pipeline might not be high right now, it is designed to easily scale to higher demands.
    4. **No Current Data Governance/Security Requirements**: Being able to anonymise or depersonalise data before loading it to the destination is very valuable, but as we are working with public data, those basic transformations can be part of the future improvements.

### Future Improvements

To make this pipeline production-ready, the following improvements should be made:

- **Arrow Backend**: Ensure that Pandas is using Arrow instead of NumPy as it's backend.
- **Scheduling & Automation**: Determine how to automate pipelines runs. This can be done by converting the pipeline into a daemon that scans the config files periodically and runs them according to their configured schedule, or using another tool such as Airflow.
- **Schema Management**: The schema management should be expanded to include:
    - Column name changes.
    - Column type conversions.
    - Basic transformations.
    - Data quality checks.
    - Schema evolution (adding and removing columns).
- **Secret Management**: Use a more secure way to manage secrets, like environment variables or a secret manager.
- **Testing**: Add unit and integration tests to ensure connections, schema operations and transformations work as expected.
- **Error Handling and Observability**: Review error handling and integrate with an observability stack for proper error tracking and monitoring of pipelines.
- **CI/CD**: Implement a CI/CD pipeline for automated testing and deployment.
- **Packaging**: Package the project properly using `pyproject.toml` for better dependency and distribution management.

## Hikeability Dashboard

Data for the dashboard is sourced from [Open Meteo](https://open-meteo.com/en/docs) via their Weather Forecast API.

[Click here to view the Hikeability report in Looker Studio](https://lookerstudio.google.com/s/m_rKKmd0W_Q)

To avoid scrolling, click the 3 dot menu button (top right), and click "Present".
