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
                .load("hdfs://master:9000/user/user/blocking/db/main_A_25p_" + size + ".csv").limit(1000);
        this.bob = spark.read().format("csv")
                .load("hdfs://master:9000/user/user/blocking/db/main_B_25p_" + size + ".csv").limit(1000);
        this.referenceSet = spark.read().format("csv").option("header", "true")
                .load("hdfs://master:9000/user/user/blocking/db/Master.csv").limit(5);
    }

    public Dataset<Row> getAlice() {
        return this.query(alice);
    }

    public Dataset<Row> getBob() {
        return this.query(bob);
    }

    public Dataset<Row> getReferenceSet() {
        return referenceSet.select(col("nameFirst").alias("col1")
                , col("birthCity").alias("col2")
                , col("nameLast").alias("col3")
        ).where("col1 is not null and col2 is not null and col3 is not null");
    }

    private Dataset<Row> query(Dataset<Row> ds){
        return ds.select(
                col("_c0").alias("id")
                , col("_c1").alias("surname")
                , col("_c2").alias("name")
                , col("_c5").alias("city")
        ).where("surname is not null and name is not null and city is not null");
    }
}
