# from __future__ import annotations

# import time
# from datetime import datetime, timedelta, timezone

# import requests
# from airflow import DAG
# from airflow.exceptions import AirflowException
# from airflow.operators.python import PythonOperator


# SPARK_REST_URL = "http://spark-master:6066"
# NAMENODE_WEBHDFS_URL = "http://namenode:9870"
# JAR_PATH = "file:///spark-hadoop-airflow/build/libs/spark-examples.jar"
# MAIN_CLASS = "com.example.spark.Ex05AirflowSpark"
# HDFS_OUTPUT_PATH = "/user/hadoop/refined_data/airflow_batch/members"

# POLL_INTERVAL_SECONDS = 10
# POLL_TIMEOUT_SECONDS = 600


# def submit_spark_job() -> str:
#     submit_result = requests.post(
#         f"{SPARK_REST_URL}/v1/submissions/create",
#         json={
#             "action": "CreateSubmissionRequest",
#             "appResource": JAR_PATH,
#             "mainClass": MAIN_CLASS,
#             "clientSparkVersion": "3.1.1",
#             "appArgs": [],
#             "environmentVariables": {},
#             "sparkProperties": {
#                 "spark.master": "spark://spark-master:7077",
#                 "spark.app.name": "Ex05AirflowSpark",
#                 "spark.submit.deployMode": "cluster",
#             },
#         },
#     ).json()

#     if not submit_result.get("success"):
#         raise AirflowException(f"spark-submit 제출 실패: {submit_result}")

#     submission_id = submit_result["submissionId"]
#     print(f">>> 제출 완료. submissionId={submission_id}")
#     return submission_id


# # 이전 task가 리턴한 submission_id를 전달받아, 완료될 때까지 상태 폴링
# def wait_for_completion(**context) -> None:
#     submission_id = context["ti"].xcom_pull(task_ids="submit_spark_job")

#     elapsed_seconds = 0
#     while elapsed_seconds < POLL_TIMEOUT_SECONDS:
#         status = requests.get(f"{SPARK_REST_URL}/v1/submissions/status/{submission_id}").json()
#         driver_state = status.get("driverState", "UNKNOWN")
#         print(f">>> [{elapsed_seconds}s] driverState={driver_state}")

#         if driver_state == "FINISHED":
#             return
#         if driver_state in ("FAILED", "ERROR", "KILLED"):
#             raise AirflowException(f"spark 배치 작업 실패: {status}")

#         time.sleep(POLL_INTERVAL_SECONDS)
#         elapsed_seconds += POLL_INTERVAL_SECONDS

#     raise AirflowException(f"{POLL_TIMEOUT_SECONDS}초 내에 끝나지 않았습니다. submissionId={submission_id}")


# # 오늘 날짜 출력 폴더에 파일이 실제로 생성됐는지 확인
# def check_result() -> None:
#     today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
#     path = f"{HDFS_OUTPUT_PATH}/{today}"

#     response = requests.get(f"{NAMENODE_WEBHDFS_URL}/webhdfs/v1{path}", params={"op": "LISTSTATUS"})
#     response.raise_for_status()
#     file_statuses = response.json()["FileStatuses"]["FileStatus"]

#     print(f">>> {path} 안에 파일 {len(file_statuses)}개 확인됨")
#     if len(file_statuses) == 0:
#         raise AirflowException(f"{path}에 파일이 없습니다.")


# dag = DAG(
#     dag_id="json_batch_to_hdfs_v2",
#     description="[수업용] submit -> wait -> check_result 3단계 task로 나눈 버전 (Ex05AirflowSpark)",
#     default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)},
#     start_date=datetime(2026, 1, 1),
#     schedule="*/5 * * * *",
#     catchup=False,
#     max_active_runs=1,
# )

# submit_task = PythonOperator(
#     task_id="submit_spark_job",
#     python_callable=submit_spark_job,
#     dag=dag,
# )

# wait_task = PythonOperator(
#     task_id="wait_for_completion",
#     python_callable=wait_for_completion,
#     dag=dag,
# )

# check_task = PythonOperator(
#     task_id="check_result",
#     python_callable=check_result,
#     dag=dag,
# )

# # 순차(직렬)작업 처리
# submit_task >> wait_task >> check_task

# # 병렬처리라면 아래와 같이 별도로 나열
# # submit_task
# # wait_task
# # check_task