import datetime
import pendulum
import boto3

from airflow.decorators import dag, task
import uuid


from common import csv_file_name, local_schema_path

from tabular_pipeline.steps.conform import Conform
from tabular_pipeline.steps.load import Load
from tabular_pipeline.read import load_schema

from dotenv import load_dotenv
import os

load_dotenv()


@dag(
    dag_id="process-remote-csv",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessRemoteCsv():
    @task
    def load(ti=None):
        s3 = boto3.resource(
            service_name="s3",
            region_name=os.environ.get("AWS_DEFAULT_REGION"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )

        obj = s3.Object(os.environ.get("AWS_BUCKET_NAME"), csv_file_name)
        file_content = obj.get()["Body"].read()
        session_id = uuid.uuid4()
        loader = Load(
            session_id=session_id, file_content=file_content, file_name=csv_file_name
        )
        loader.run()
        return str(session_id)

    @task
    def conform(ti=None):
        session_id = ti.xcom_pull(key="return_value", task_ids="load")
        schema = load_schema(session_id, local_schema_path)
        conformer = Conform(session_id=session_id, schema=schema)
        conformer.run()

    load() >> conform()


dag = ProcessRemoteCsv()
