# Airflow test

This is a simple setup that runs sample pipelines in airflow. It uses the package [tabular-pipeline](https://github.com/franciscobmacedo/tabular-pipeline) as a small example of a pipeline that reads and conforms files.

There are 4 dags that perform similar things:

- process local csv
- process local xlsx
- process remote csv
- process remote xlsx

# How to run
- mkdir -p ./logs ./plugins ./outputs
- echo -e "AIRFLOW_UID=$(id -u)" > .env
- edit the rest of the `.env` file with the S3 variables from `.env.sample`.
- docker-compose up --build
- Navigate to https://localhost:8080 for the airflow interface and run whatever dag you would like.
