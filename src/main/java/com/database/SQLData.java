package com.database;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SQLData {
    private final Dataset<Row> alice;
    private final Dataset<Row> bob;
    private final Dataset<Row> referenceSet;
    SparkSession spark;

    public SQLData(SparkSession spark, String size) {
        // Get SparkSession from Main
        this.spark = spark;
        this.alice = spark.read().format("csv")
                .load("hdfs://master:9000/user/user/blocking/db/main_A_25p_" + size + ".csv").limit(2);
        this.bob = spark.read().format("csv")
                .load("hdfs://master:9000/user/user/blocking/db/main_B_25p_" + size + ".csv").limit(2);
        this.referenceSet = spark.read().format("csv")
                .load("hdfs://master:9000/user/user/blocking/db/main_A_authors3.csv").limit(4);
    }

    public Dataset<Row> getAlice() {
        return this.query(alice);
    }

    public Dataset<Row> getBob() {
        return this.query(bob);
    }

    public Dataset<Row> getReferenceSet() {
        return referenceSet;
    }

    private Dataset<Row> query(Dataset<Row> ds){
        return ds.select(
                col("_c0").alias("id")
                , col("_c1").alias("surname")
                , col("_c2").alias("name")
                , col("_c5").alias("city")
        );
    }
}
