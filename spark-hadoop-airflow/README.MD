빌드/실행 순서
1. java 및 gradle setting
1-1)intellij의 project structure에서 java21설치 
1-2)intellij에서 gradle파일로 open

2. jar 빌드 (spark-hadoop-airflow 폴더에서)
./gradlew build

3. 예제 실행 (예: Ex01)

docker exec -it spark-master /spark/bin/spark-submit --class com.example.spark.Ex01DataFrameBasics --master spark://spark-master:7077 /spark-hadoop-airflow/build/libs/spark-examples.jar   
