package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

// Airflow가 spark-master REST Submission API(6066)로 이 클래스를 실행 요청
// 원본 Parquet 중 오늘 날짜 폴더만 읽어 정제 후, 오늘 날짜 폴더에 다시 Parquet로 저장
public class Ex05AirflowSpark {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("AirflowMemberBatchProcessor")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // 오늘날짜 추출
        String today = LocalDate.now().toString();

        // Ex04KafkaJsonStreaming이 저장한 원본 Parquet 중 오늘 날짜 폴더만 읽기
        String hdfsRawPath = "hdfs://namenode:8020/user/hadoop/refined_data/member_stream/" + today;
        Dataset<Row> rawDf;
        try {
            rawDf = spark.read().parquet(hdfsRawPath);
        } catch (Exception e) {
            System.out.println(">>> " + today + " 폴더가 아직 없어 종료합니다: " + hdfsRawPath);
            spark.stop();
            return;
        }

        // [데이터 정제] (복잡한연산이라 가정) age가 19세 이상이면 isAdult를 true, 아니면 false로 지정
        Dataset<Row> refinedDf = rawDf
                .withColumn("isAdult", when(col("age").geq(19), true).otherwise(false));

        // 정제 데이터를 오늘 날짜 폴더에 Parquet로 저장 (같은 날 재실행해도 오늘 폴더만 덮어씀)
        String hdfsRefinedPath = "hdfs://namenode:8020/user/hadoop/refined_data/airflow_batch/members/" + today;
        refinedDf.write().mode("overwrite").parquet(hdfsRefinedPath);
        System.out.println(">>> 정제된 데이터를 HDFS에 저장했습니다 (Parquet): " + hdfsRefinedPath);

        spark.stop();
    }
}
