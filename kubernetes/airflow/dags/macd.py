import datetime
import json
import random

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import BaseOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# with DAG(
#         dag_id="my_dag_name",
#         start_date=datetime.datetime(2021, 1, 1),
#         schedule="@daily",
# ):
#     EmptyOperator(task_id="task")

class LatestMACDOperator(BaseOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        print("asdf 004")
        lastest_macd_time = datetime.datetime(2021, 1, 1)
        print("lastest_macd_time =", lastest_macd_time)
        return lastest_macd_time

def execution_id_function():
    print("execution_id 001")
    execution_id = str(random.randint(1, 1000))
    print("execution_id =", execution_id)
    return execution_id

def lastest_macd_time_function(random_base):
    print("asdf 005")
    lastest_macd_time = datetime.datetime(2021, 1, 1)
    lastest_macd_seconds_since_epoch = int(lastest_macd_time.timestamp())
    print("lastest_macd_seconds_since_epoch =", lastest_macd_seconds_since_epoch)
    return lastest_macd_seconds_since_epoch

with DAG(
    dag_id="MACD",
    schedule=datetime.timedelta(hours=4),
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=["technical indicator"],
) as dag:
    # latest_macd = LatestMACDOperator(name="my_001", task_id="latest_macd")
    execution_id_task = PythonOperator(task_id="execution_id_task", python_callable=execution_id_function, op_kwargs={})
    latest_macd = PythonOperator(task_id="latest_macd", python_callable=lastest_macd_time_function, op_kwargs={"random_base": 1})
    latest_ticks_ma10s = HttpOperator(
        task_id="latest_ticks_ma10s",
        http_conn_id="spark_http_conn_id",
        method="POST",
        endpoint="/v1/submissions/create",
        data="""
{
  "action": "CreateSubmissionRequest",
  "clientSparkVersion": "3.5.0",
  "appResource": "file:/opt/spark/jars/spark-iceberg-1.0.0.jar",
  "mainClass": "com.hwn.bd25.sparkiceberg.SparkIcebergJob",
  "environmentVariables": {
    "SPARK_ENV_LOADED": "1"
  },
  "supervise": false,
  "appArgs": [
    "{{ti.xcom_pull(task_ids='execution_id_task')}}", "{{ti.xcom_pull(task_ids='latest_macd')}}"
  ],
  "sparkProperties": {
    "spark.driver.supervise": "false",
    "spark.app.name": "SparkIcebergJob",
    "spark.eventLog.enabled": "false",
    "spark.submit.deployMode": "cluster",
    "spark.master": "spark://spark-master:7077",
    "spark.executor.instances": "1",
    "spark.executor.memory": "500m",
    "spark.executor.cores": "1",
    "spark.driver.memory": "450m",
    "spark.dynamicAllocation.enabled": "false"
  }
}
        """,
        headers={"Content-Type": "application/json"}
    )
    wait_for_ma10s = S3KeySensor(
        task_id='wait_for_ma10s',
        bucket_name='bigdata25-rates-glue',
        bucket_key="airflow/{{ti.xcom_pull(task_ids='execution_id_task')}}_ma10s.json",
        aws_conn_id='aws_my',
        poke_interval=10,      # Check every 30 seconds
        timeout=60 * 60,       # Timeout after 1 hour (3600 seconds)
        mode='reschedule'      # Recommended for long waits to free up worker slots
    )
    latest_ma5 = HttpOperator(
        task_id="latest_ma5",
        http_conn_id="spark_http_conn_id",
        method="POST",
        endpoint="/v1/submissions/create",
        data="""
    {
      "action": "CreateSubmissionRequest",
      "clientSparkVersion": "3.5.0",
      "appResource": "file:/opt/spark/jars/spark-clickhouse-source-1.0.0.jar",
      "mainClass": "com.hwn.bd25.sparkclickhouse.SparkClickhouseSourceJob",
      "environmentVariables": {
        "SPARK_ENV_LOADED": "1"
      },
      "supervise": false,
      "appArgs": [
        "{{ti.xcom_pull(task_ids='execution_id_task')}}", "{{ti.xcom_pull(task_ids='latest_macd')}}"
      ],
      "sparkProperties": {
        "spark.driver.supervise": "false",
        "spark.app.name": "SparkClickhouseSourceJob",
        "spark.eventLog.enabled": "false",
        "spark.submit.deployMode": "cluster",
        "spark.master": "spark://spark-master:7077",
        "spark.executor.instances": "1",
        "spark.executor.memory": "500m",
        "spark.executor.cores": "1",
        "spark.driver.memory": "450m",
        "spark.dynamicAllocation.enabled": "false"
      }
    }
            """,
        headers={"Content-Type": "application/json"}
    )
    wait_for_ma5s = S3KeySensor(
        task_id='wait_for_ma5s',
        bucket_name='bigdata25-rates-glue',
        bucket_key="airflow/{{ti.xcom_pull(task_ids='execution_id_task')}}_ma5s.json",
        aws_conn_id='aws_my',
        poke_interval=10,  # Check every 30 seconds
        timeout=60 * 60,  # Timeout after 1 hour (3600 seconds)
        mode='reschedule'  # Recommended for long waits to free up worker slots
    )
    macd = EmptyOperator(task_id="macd")

    execution_id_task >> latest_macd
    latest_macd >> [latest_ticks_ma10s, latest_ma5]
    latest_ticks_ma10s >> wait_for_ma10s >> macd
    latest_ma5 >> wait_for_ma5s >> macd


