package com.database;


import com.utils.Conf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SQLData {
    private final Dataset<Row> alice;
    private final Dataset<Row> bob;
    private final Dataset<Row> referenceSet;
    SparkSession spark;

    public SQLData(SparkSession spark) {
        // Get SparkSession from Main
        this.spark = spark;

        this.alice = spark.read().format("csv")
                .load(Conf.HDFS_DIRECTORY + Conf.DB_A_FILE_NAME)
                .limit(Conf.DB_SIZE);

        this.bob = spark.read().format("csv")
                .load(Conf.HDFS_DIRECTORY + Conf.DB_B_FILE_NAME)
                .limit(Conf.DB_SIZE);

        this.referenceSet = spark.read().format("csv").option("header", "true")
                .load(Conf.HDFS_DIRECTORY + Conf.RS_FILE_NAME);

        System.out.println("Data from DB loaded");
    }

    public Dataset<Row> getAlice() {
        return this.query(alice);
    }

    public Dataset<Row> getBob() {
        return this.query(bob);
    }

    public Dataset<Row> getReferenceSet() {
        return referenceSet.select(
                col("nameFirst").alias("col1")
                , col("birthCity").alias("col2")
                , col("nameLast").alias("col3")
        ).where("col1 is not null and col2 is not null and col3 is not null");
    }

    private Dataset<Row> query(Dataset<Row> ds){
        return ds.select(
                col("_c0").alias(Conf.ID)
                , col("_c1").alias(Conf.ATTR_1)
                , col("_c2").alias(Conf.ATTR_2)
                , col("_c5").alias(Conf.ATTR_3)
        ).where(Conf.ATTR_1 + " is not null" +
                " and " + Conf.ATTR_2 + " is not null" +
                " and " + Conf.ATTR_3 + " is not null");
    }
}
