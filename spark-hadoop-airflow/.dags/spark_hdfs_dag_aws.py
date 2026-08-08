from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# S3 및 EMR 설정
S3_JAR_PATH = "s3://your-bucket-name/artifacts/spark-examples.jar"
MAIN_CLASS = "com.example.spark.Ex05AirflowSpark"
JOB_FLOW_ID = "j-XXXXXXXXX" 

# EMR Step 정의 (Spark Submit을 대체)
SPARK_STEPS = [
    {
        "Name": "Ex05AirflowSpark-Step",
        "ActionOnFailure": "TERMINATE_CLUSTER",
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
    dag_id="json_batch_to_hdfs_emr",
    description="EMR을 활용한 member_stream Parquet 정제 배치",
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
)

# 이미 실행 중인 EMR 클러스터에 Step만 추가
add_spark_step = EmrAddStepsOperator(
    task_id="add_spark_step",
    job_flow_id=JOB_FLOW_ID,
    steps=SPARK_STEPS,
    aws_conn_id="aws_default",
    dag=dag,
)

add_spark_step