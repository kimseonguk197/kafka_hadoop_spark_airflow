package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.VoidFunction2;

import static org.apache.spark.sql.functions.*;


public class Ex04KafkaJsonStreaming {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("KafkaMemberStreamingProcessor")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        // producer의 MemberDto의 JSON에 맞는 스키마 구조
        // Streaming을 통해 받는 json데이터는 스키마 필요
        StructType memberSchema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("email", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("createdAt", DataTypes.StringType, true)
        });

        System.out.println(">>> Ex04KafkaJsonStreaming 시작: member-topic 구독 대기 중...");

        // 1. member-topic 구독
        // consumer group id의 경우 spark-kafka-source-<UUID>-... 형태로 자동 생성
        // offset-reset은 latest 기본
        String bootstrapServers = "host.docker.internal:29092";
        Dataset<Row> kafkaDf = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "member-topic")
                .load();

        // 2. Kafka record의 value(byte[])를 문자열로 바꾼 뒤 JSON 파싱
        Dataset<Row> memberDf = kafkaDf
                .selectExpr("CAST(value AS STRING) AS json_value")
                .select(from_json(col("json_value"), memberSchema).as("data"))
                .select("data.*");

        // 3. HDFS(Parquet)에 저장
        String hdfsRefinedPath = "hdfs://namenode:8020/user/hadoop/refined_data/member_stream/";
        // checkpointLocation: 데이터가 아니라 이 스트리밍 쿼리의 진행 상태가 기록되는 곳
        String checkpointLocationPath = "hdfs://namenode:8020/user/hadoop/checkpoints/";
        StreamingQuery query = memberDf.writeStream()
                .outputMode("append")
                .option("checkpointLocation", checkpointLocationPath)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDf, batchId) -> {
                    long count = batchDf.count();
                    System.out.println(">>> [batch " + batchId + "] 수신 메시지 건수: " + count);
                    if (count > 0) {
                        // 오늘날짜별 폴더로 분류하여 저장
                        String today = java.time.LocalDate.now().toString();
                        String todayPath = hdfsRefinedPath + today + "/";
                        batchDf.show(false);
                        batchDf.write().mode("append").parquet(todayPath);
                        System.out.println(">>> [batch " + batchId + "] HDFS 저장 완료: " + todayPath);
                    }
                })
                .start();

        query.awaitTermination();
    }
}