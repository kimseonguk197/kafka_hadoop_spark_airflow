from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator

# S3 및 EMR 설정
S3_JAR_PATH = "s3://my-kafka-spark-airflow-bucket-346903264902-ap-northeast-2-an/artifacts/spark-examples.jar"
MAIN_CLASS = "com.example.spark.Ex04KafkaJsonStreaming"
JOB_FLOW_ID = "j-06607543FDH1KDCUH54Q"

# EMR Step 정의
# 로컬에서처럼 직접 http요청을 보내는 방식이 아니라, emr의 작업(step)을 신규로 등록할것을 요청
# jar 실행은 emr에서 s3에 넣어둔 jar를 실행하는 방식
SPARK_STEPS = [
    {
        "Name": "Ex04KafkaJsonStreaming-Step",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--class", MAIN_CLASS,
                "--deploy-mode", "cluster",
                "--master", "yarn",
                S3_JAR_PATH,
            ],
        },
    }
]

dag = DAG(
    dag_id="kafka_streaming_start_emr",
    description="[AWS EMR] Ex04KafkaJsonStreaming(상시 streaming) 시작 - 스케쥴 없이 1회성 수동 실행용",
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=datetime(2026, 1, 1),
    schedule=None,  # 반복 스케쥴 없음: 필요할 때 UI에서 수동으로 Trigger
    catchup=False,
    max_active_runs=1,
)

# 이미 실행 중인 EMR 클러스터에 Step만 추가
add_streaming_step = EmrAddStepsOperator(
    task_id="add_streaming_step",
    job_flow_id=JOB_FLOW_ID,
    steps=SPARK_STEPS,
    aws_conn_id="aws_default",
    dag=dag,
)

add_streaming_step
