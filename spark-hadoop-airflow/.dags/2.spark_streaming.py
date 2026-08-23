from __future__ import annotations

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


# spark-master의 REST Submission API(6066)을 호출하여 Ex04KafkaJsonStreaming 실행 요청
SPARK_REST_URL = "http://spark-master:6066"
JAR_PATH = "file:///spark-hadoop-airflow/build/libs/spark-examples.jar"
MAIN_CLASS = "com.example.spark.Ex04KafkaJsonStreaming"
def submit_streaming_job() -> None:
    submit_result = requests.post(
        f"{SPARK_REST_URL}/v1/submissions/create",
        json={
            "action": "CreateSubmissionRequest",
            "appResource": JAR_PATH,
            "mainClass": MAIN_CLASS,
            "clientSparkVersion": "3.3.0",
            "appArgs": [],
            "environmentVariables": {},
            "sparkProperties": {
                "spark.master": "spark://spark-master:7077",
                "spark.app.name": "KafkaMemberStreamingProcessor",
                "spark.submit.deployMode": "cluster",
            },
        },
    ).json()

    if not submit_result.get("success"):
        raise AirflowException(f"spark-submit 제출 실패: {submit_result}")

    submission_id = submit_result["submissionId"]
    print(f">>> streaming job 제출 완료 (종료되지 않고 계속 실행됨). submissionId={submission_id}")
    
dag = DAG(
    dag_id="kafka_streaming_start",
    description="[로컬] Ex04KafkaJsonStreaming(상시 streaming) 시작 - 스케쥴 없이 1회성 수동 실행용",
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=datetime(2026, 1, 1),
    schedule=None,  # 반복 스케쥴 없음
    catchup=False,
    max_active_runs=1,
)

# PythonOperator를 통해 submit_streaming_job 함수 실행
submit_streaming_task = PythonOperator(
    task_id="submit_streaming_job",
    python_callable=submit_streaming_job,
    dag=dag,
)

# 실행
submit_streaming_task

# 순차(직렬)작업 처리
# task1 >> task2 >> task3

# 병렬처리라면 아래와 같이 나열
# task1
# task2
# task3