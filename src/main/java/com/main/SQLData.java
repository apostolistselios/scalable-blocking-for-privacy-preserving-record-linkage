package com.main;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SQLData {
    SparkConf conf;

    public SQLData(SparkConf conf) {
        // Get SparkConf from Main
        this.conf = conf;
    }

    public void startSQL(){
        // Create DB session
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Create and show demo dataset
        Dataset<Row> demo = spark.read().format("jdbc")
                .option("url", "jdbc:sqlite:test.db")
                .option("driver", "org.sqlite.JDBC")
                // Demo query: top 10 rows
                .option("query",
                        "select * from A_25p_1k\n" +
                        "order by 1\n" +
                        "limit 10")
                .load();

        demo.show();
    }
}
