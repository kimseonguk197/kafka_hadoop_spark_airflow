package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

// Spark DataFrame의 기본 생명주기: 생성 -> 가공 -> 분석
// 테스트 : docker exec -it spark-master /spark/bin/spark-submit --class com.example.spark.Ex01DataFrameBasics --master spark://spark-master:7077 /spark-process/build/libs/spark-examples.jar  
public class Ex01DataFrameBasics {
    public static void main(String[] args) {
        // 1. SparkSession: Spark 기능의 진입점(Entry Point).
        // SQL 실행, 데이터 읽기/쓰기, 클러스터 설정 관리 등 모든 작업이 이 작업 객체를 통해 처리
        SparkSession spark = SparkSession.builder()
                .appName("DataFrameBasics") // 작업의 이름
                .getOrCreate();             // 기존 세션이 있으면 가져오고, 없으면 새로 생성

        spark.sparkContext().setLogLevel("WARN"); //로그레벨조절을 통해 과한 로그출력방지

        // 2. StructType(스키마): RDBMS의 테이블 구조와 동일.
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("email", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
        });

        // 3. 데이터 준비
        List<Row> data = Arrays.asList(
                RowFactory.create("Alice", "alice@gmail.com", 28),
                RowFactory.create("Bob", "bob@naver.com", 17),
                RowFactory.create("Charlie", "charlie@daum.net", 34)
        );

        // 4. DataFrame 생성: 스키마와 데이터가 결합되어 분산 처리 준비 완료.
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // 5. 스키마 확인: 정의한 구조가 정확히 반영되었는지 메타데이터 확인.
        System.out.println(">>> 스키마 확인");
        df.printSchema();

        // transformation 작업 : select, filter, orderby 등
        // 6. select: 필요한 컬럼만 추출. SQL의 SELECT 문과 동일.
        System.out.println(">>> select(name, age)");
        df.select("name", "age").show();

        // 7. filter(Selection): 조건을 만족하는 로우만 추출. Spark는 이 필터링을 로컬 노드에서 먼저 수행
        System.out.println(">>> filter(age >= 19)");
        df.filter(col("age").geq(19)).show();

        // 8. orderBy(Sort): 데이터 정렬. 분산 환경에서는 데이터가 여러 노드에 나뉘어 있으므로 
        // 정렬 시 특정 노드로 데이터를 모으는(Shuffle) 과정이 발생함.
        System.out.println(">>> orderBy(age desc)");
        df.orderBy(col("age").desc()).show();

        // action 작업 : show, write 등
        // 9. 데이터 출력: show()는 Action 작업으로, 실제 클러스터 노드들에서 데이터를 모아 결과값을 출력함.
        System.out.println(">>> 전체 데이터");
        df.show();

        // 10. 세션 종료: 클러스터 자원 반환.
        spark.stop();

    }
}