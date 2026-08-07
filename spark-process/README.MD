빌드/실행 순서
1. java 및 gradle setting
1-1)intellij의 project structure에서 java21설치 
1-2)intellij에서 gradle파일로 open

2. jar 빌드 (spark-process 폴더에서)
./gradlew build

3. 예제 실행 (예: Ex01)

docker exec -it spark-master /spark/bin/spark-submit --class com.example.spark.Ex01DataFrameBasics --master spark://spark-master:7077 /spark-process/build/libs/spark-examples.jar   

<!-- 위 명령어가 실행가능한 이유 -->
1./spark/bin/spark-submit은 spark-master 컨테이너 이미지안에 이미 설치되어 있는 Spark 배포판의 실행 스크립트
=> Spark 애플리케이션은 최초 실행시 드라이버(Driver) 역할을 하는 프로세스가 필요. spark-submit은 이 드라이버 프로그램을 구동하고 SparkContext를 초기화하는 일련의 복잡한 과정을 대신 처리
2. docker-compose.yml에서 /spark-process 폴더 전체를 마운트 하고 있기에 그 안에 build/libs/spark-examples.jar를 실행할수 있음. 그 jar안에 com.example... 의 class파일을 실행.
3.--master를 생략하면 spark-submit은 기본값인 local[*] 모드로 동작. 즉 spark-master 컨테이너 안에서 실행되긴 하지만, spark-worker와는 전혀 무관하게 그 컨테이너 프로세스 하나(로컬 스레드)만으로 job을 실행.