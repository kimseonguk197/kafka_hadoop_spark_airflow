package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.when;

public class Ex02JsonHdfsSave {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("MemberMessageProcessor")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // 컨테이너에 마운트된 원본 JSON 데이터 읽기
        String inputPath = "/test_data/members.json";
        // Spark의 스키마 추론 기능 덕분에 스키마를 자동으로 정의
        Dataset<Row> rawDf = spark.read().json(inputPath);
        System.out.println(">>> 데이터 구조:");
        rawDf.printSchema();
        rawDf.show();

        // HDFS의 raw_data 폴더에 원본 JSON 그대로 저장
        String hdfsRawPath = "hdfs://namenode:8020/user/hadoop/raw_data/members";
        rawDf.write().mode("overwrite").json(hdfsRawPath);
        System.out.println(">>> 원본 데이터를 HDFS에 백업: " + hdfsRawPath);

        // [데이터 정제]
        // - age가 19세 이상이면 'isAdult'를 true, 아니면 false로 지정
        Dataset<Row> refinedDf = rawDf
                .withColumn("isAdult", when(col("age").geq(19), true).otherwise(false));

        System.out.println(">>> 정제된 데이터 구조:");
        refinedDf.printSchema();
        refinedDf.show();

        // [정제 데이터 저장] HDFS의  고성능 압축 포맷인 Parquet형태로 저장
        String hdfsRefinedPath = "hdfs://namenode:8020/user/hadoop/refined_data/members";
        refinedDf.write().mode("overwrite").parquet(hdfsRefinedPath);
        System.out.println(">>> 정제된 데이터를 HDFS에 저장했습니다 (Parquet): " + hdfsRefinedPath);

        // [검증] 저장 직후 다시 읽어서 정상 저장 여부 확인
        System.out.println(">>> 저장된 원본(JSON) 재조회:");
        spark.read().json(hdfsRawPath).show();

        System.out.println(">>> 저장된 정제 데이터(Parquet) 재조회:");
        spark.read().parquet(hdfsRefinedPath).show();

        spark.stop();
    }
}
