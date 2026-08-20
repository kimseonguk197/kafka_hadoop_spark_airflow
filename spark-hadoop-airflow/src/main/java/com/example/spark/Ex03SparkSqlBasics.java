package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;


// hdfs에 저장된 데이터를 불러서 조회/통계 처리
public class Ex03SparkSqlBasics {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLBasics")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        // Ex02JsonHdfsSave에서 정제 후 저장한 Parquet 데이터를 HDFS에서 로드
        String hdfsRefinedPath = "hdfs://namenode:8020/user/hadoop/refined_data/members";
        Dataset<Row> rawDf = spark.read().parquet(hdfsRefinedPath);

        // emailDomain 컬럼 추가
        Dataset<Row> df = rawDf
                .withColumn("emailDomain", split(col("email"), "@").getItem(1));

        // SQL조회를 위한 임시뷰(테이블) 생성 
        df.createOrReplaceTempView("members");

        System.out.println(">>> SQL: 전체 조회");
        spark.sql("SELECT * FROM members").show();

        System.out.println(">>> SQL: 성인 회원만 조회");
        spark.sql("SELECT name, age FROM members WHERE isAdult = true").show();

        // 복잡한 집계, 정렬 SQL문 가능
        System.out.println(">>> SQL: 도메인별 회원 수 (GROUP BY)");
        spark.sql(
                "SELECT emailDomain, COUNT(*) AS member_count " +
                        "FROM members " +
                        "GROUP BY emailDomain " +
                        "ORDER BY member_count DESC"
        ).show();

        // SQL문으로 JOIN 수행도 가능
        System.out.println(">>> [SQL 방식] inner join");
        String hdfsOrdersRefinedPath = "hdfs://namenode:8020/user/hadoop/refined_data/orders";
        Dataset<Row> ordersDf = spark.read().parquet(hdfsOrdersRefinedPath);
        ordersDf.createOrReplaceTempView("orders");
        spark.sql(
                "SELECT o.orderId, m.name, m.email, o.item, o.price, o.orderedAt " +
                        "FROM members m " +
                        "JOIN orders o ON m.email = o.memberEmail"
        ).show();

        // DataFrame API와 SQL은 동일한 엔진사용하여 유사한 기능 수행
        System.out.println(">>> [비교] 성인 회원 조회 (SQL 방식)");
        spark.sql("SELECT name, age FROM members WHERE isAdult = true").show();

        System.out.println(">>> [비교] 성인 회원 조회 (DataFrame API 방식)");
        df.filter(col("isAdult").equalTo(true))
          .select("name", "age")
          .show();

        spark.stop();
    }
}
