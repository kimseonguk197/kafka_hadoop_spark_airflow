package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;


// Ex01DataFrameBasic에서 데이터를 처리한 방식은 dataframe API였고, 여기서는 직접 SQL문을 활용
public class Ex03SparkSqlBasics {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLBasics")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        // 1. 데이터 로드
        Dataset<Row> rawDf = spark.read().json("/test_data/members.json");

        Dataset<Row> df = rawDf
                .withColumn("emailDomain", split(col("email"), "@").getItem(1))
                .withColumn("isAdult", when(col("age").geq(19), true).otherwise(false));

        // DataFrame을 임시 뷰(테이블)로 등록한 후 SQL로 조회
        df.createOrReplaceTempView("members");

        System.out.println(">>> SQL: 전체 조회");
        spark.sql("SELECT * FROM members").show();

        System.out.println(">>> SQL: 성인 회원만 조회");
        spark.sql("SELECT name, age FROM members WHERE isAdult = true").show();

        System.out.println(">>> SQL: 도메인별 회원 수 (GROUP BY)");
        spark.sql(
                "SELECT emailDomain, COUNT(*) AS member_count " +
                        "FROM members " +
                        "GROUP BY emailDomain " +
                        "ORDER BY member_count DESC"
        ).show();

        // 2. SQL문으로 JOIN 수행
        System.out.println(">>> [SQL 방식] inner join");
        Dataset<Row> ordersDf = spark.read().json("/test_data/orders.json");
        ordersDf.createOrReplaceTempView("orders");
        spark.sql(
                "SELECT o.orderId, m.name, m.email, o.item, o.price, o.orderedAt " +
                        "FROM members m " +
                        "JOIN orders o ON m.email = o.memberEmail"
        ).show();

        // DataFrame API와 SQL은 동일한 엔진사용
        System.out.println(">>> [비교] 성인 회원 조회 (SQL 방식)");
        spark.sql("SELECT name, age FROM members WHERE isAdult = true").show();

        System.out.println(">>> [비교] 성인 회원 조회 (DataFrame API 방식)");
        df.filter(col("isAdult").equalTo(true))
          .select("name", "age")
          .show();

        spark.stop();
    }
}
