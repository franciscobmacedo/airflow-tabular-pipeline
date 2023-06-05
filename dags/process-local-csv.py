import datetime
import pendulum

from common import csv_local_file_path, local_schema_path
from airflow.decorators import dag, task
import uuid

from tabular_pipeline.steps.conform import Conform
from tabular_pipeline.steps.load import Load
from tabular_pipeline.read import load_schema


@dag(
    dag_id="process-local-csv",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessLocalCsv():
    @task
    def load(ti=None):
        session_id = uuid.uuid4()
        loader = Load(session_id=session_id, file_path=csv_local_file_path)
        loader.run()
        return str(session_id)

    @task
    def conform(ti=None):
        session_id = ti.xcom_pull(key="return_value", task_ids="load")
        schema = load_schema(session_id, local_schema_path)
        conformer = Conform(session_id=session_id, schema=schema)
        conformer.run()

    load() >> conform()


dag = ProcessLocalCsv()
