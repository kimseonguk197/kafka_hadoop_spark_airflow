from __future__ import annotations

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

# spark-master의 REST Submission API(6066)을 호출하여 Ex05AirflowSpark파일 실행 요청
SPARK_REST_URL = "http://spark-master:6066"
JAR_PATH = "file:///spark-hadoop-airflow/build/libs/spark-examples.jar"
MAIN_CLASS = "com.example.spark.Ex05AirflowSpark"


def run_spark_batch_via_rest() -> None:
    submit_result = requests.post(
        f"{SPARK_REST_URL}/v1/submissions/create",
        json={
            "action": "CreateSubmissionRequest",
            "appResource": JAR_PATH,
            "mainClass": MAIN_CLASS,
            "clientSparkVersion": "3.1.1",
            "appArgs": [],
            "environmentVariables": {},
            "sparkProperties": {
                "spark.master": "spark://spark-master:7077",
                "spark.app.name": "Ex05AirflowSpark",
                "spark.submit.deployMode": "cluster",
            },
        },
    ).json()

    if not submit_result.get("success"):
        raise AirflowException(f"spark-submit 제출 실패: {submit_result}")

    print("제출 완료")


dag = DAG(
    dag_id="json_batch_to_hdfs",
    description="member_stream Parquet을 정제해 airflow_batch 경로에 다시 저장 (Ex05AirflowSpark)",
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=datetime(2026, 1, 1),
    # schedule="0 0 * * *", #매일 자정
    schedule="*/5 * * * *", #5분에 한번
    catchup=False,
    max_active_runs=1,
)

# PythonOperator를 통해 run_spark_batch_via_rest 함수 실행
run_spark_batch_task = PythonOperator(
    task_id="run_spark_batch_via_rest",
    python_callable=run_spark_batch_via_rest,
    dag=dag,
)
