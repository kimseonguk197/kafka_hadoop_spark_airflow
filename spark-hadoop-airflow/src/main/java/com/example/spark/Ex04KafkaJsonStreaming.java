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
        // Streaming을 통해 받는 json데이터는 스키마 사전 정의 필요
        StructType memberSchema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("email", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("createdAt", DataTypes.StringType, true)
        });

        System.out.println(">>> Ex04KafkaJsonStreaming 시작: member-topic 구독 대기 중...");

        // kafka의 member-topic 구독
       String bootstrapServers = "host.docker.internal:29092";
        // String bootstrapServers = "b-1.mymsk.in0pxt.c4.kafka.ap-northeast-2.amazonaws.com:9092,b-2.mymsk.in0pxt.c4.kafka.ap-northeast-2.amazonaws.com:9092";
        Dataset<Row> kafkaDf = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "member-topic")
                // consumer group id의 경우 spark-kafka-source-<UUID>-... 형태로 자동 생성
                // offset-reset은 latest 기본
                .load();

        // Kafka record의 JSON 문자를 문자열로 바꾼 뒤 Dataset으로 변환
        Dataset<Row> memberDf = kafkaDf
                .selectExpr("CAST(value AS STRING) AS json_value")
                .select(from_json(col("json_value"), memberSchema).as("data"))
                .select("data.*");

        // 3. HDFS(Parquet)에 저장
        String hdfsRefinedPath = "hdfs://namenode:8020/user/hadoop/refined_data/member_stream/";
//        String hdfsRefinedPath = "s3://my-kafka-spark-airflow-bucket-346903264902-ap-northeast-2-an/user/hadoop/refined_data/member_stream/";
        String checkpointLocationPath = "hdfs://namenode:8020/user/hadoop/checkpoints/member_stream/";
//        String checkpointLocationPath = "s3://my-kafka-spark-airflow-bucket-346903264902-ap-northeast-2-an/user/hadoop/checkpoints/member_stream/";
        StreamingQuery query = memberDf.writeStream()
                .outputMode("append")
                // checkpointLocation: 데이터가 아니라 이 스트리밍 쿼리의 진행 상태가 기록되는 곳
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