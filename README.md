# Hikeable
A generic data pipeline paired with a dashboard to check the weather and hike-ability of a city

The data pipeline portion is a script that takes a relative path to a JSON config file as an argument and will fetch data from a source according to the configs in the JSON file.
The script then writes the data to the specified destination.

The script can handle deltas either based only on primary key, or all columns.

To update the data in the destination, simply run the pipeline again using the same command

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

### Future Improvements

To make this pipeline production-ready, the following improvements need to be made:

*   **Schema Management**: The current schema handling is basic. It should be extended to allow for:
    *   Mapping between source and destination column names.
    *   Column type conversions.
    *   Data transformations.
    *   Data quality checks.
*   **Configuration Management**: Use a more secure way to manage secrets, like environment variables or a secret manager.
*   **Testing**: Add unit and integration tests to ensure connections work as expected and that schema operations and transformations work as expected.
*   **Error Handling and Observability**: Review error handling and integrate with an observability stack for proper error tracking and monitoring of pipelines.
*   **CI/CD**: Implement a CI/CD pipeline for automated testing and deployment.
*   **Packaging**: Package the project properly using `pyproject.toml` for better dependency and distribution management.