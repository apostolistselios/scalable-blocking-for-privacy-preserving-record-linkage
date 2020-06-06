package com.simulator;


import com.algorithms.MetaBlocking;
import com.algorithms.ReferenceSetBlocking;
import com.database.SQLData;
import com.model.Block;
import com.utils.Conf;
import com.utils.Transformations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Scanner;

public class Simulator {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("JavaRDD")
                .set("spark.executor.memory", "6g");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        SQLData db = new SQLData(spark);

        JavaRDD<List<String>> AlicesRDD = Transformations.datasetToRDD(db.getAlice(), "A");
        JavaRDD<List<String>> BobsRDD = Transformations.datasetToRDD(db.getBob(), "B");

        Dataset<Row> ReferenceSets = db.getReferenceSet();

        System.out.println("Data from DB loaded");
        long t0 = System.currentTimeMillis();

        /*
         * data in BobsRDD is like
         * [AT24345, ABEDE, ELILA, BURLINcTON]
         * [AA181290, ACO0TA, BARBRAA, BURLNGTON]
         */

        ReferenceSetBlocking rsb = new ReferenceSetBlocking();

        JavaRDD<Block> blocks = rsb.blocking(AlicesRDD, BobsRDD, ReferenceSets);

        MetaBlocking mb = new MetaBlocking();

        // Create datasets with records' blooms filters
        // define the schema for blooms dataset
        StructType bloomsFilterSchema = new StructType();
        bloomsFilterSchema = bloomsFilterSchema.add("recordID", DataTypes.StringType, false);
        bloomsFilterSchema = bloomsFilterSchema.add("bloom", DataTypes.BinaryType , false);
        ExpressionEncoder<Row> bloomsFilterEncoder = RowEncoder.apply(bloomsFilterSchema);

        Dataset<Row> AliceBloomsDS = spark.createDataset(AlicesRDD.map(mb::createBloomFilters).rdd(),bloomsFilterEncoder) ;
        Dataset<Row> BobsBloomsDS = spark.createDataset(BobsRDD.map(mb::createBloomFilters).rdd(),bloomsFilterEncoder) ;



        // define the schema for possible matches dataset
        StructType possiblesMatchesSchema = new StructType();
        // this is the column for records from Alice's database
        possiblesMatchesSchema = possiblesMatchesSchema.add("record1", DataTypes.StringType, false);
        // this is the column for records from Bob;s database
        possiblesMatchesSchema = possiblesMatchesSchema.add("record2", DataTypes.StringType, false);
        ExpressionEncoder<Row> possibleMatchesEncoder = RowEncoder.apply(possiblesMatchesSchema);

        // create possibleMatchesDS that contains only unique rows
        Dataset<Row> possibleMatchesDS = spark.createDataset(blocks.flatMap(mb::createPossibleMatches).rdd(),
                possibleMatchesEncoder).distinct() ;

        Dataset<Row> possibleMatchesWithBloomsDS = possibleMatchesDS.join(AliceBloomsDS,
                possibleMatchesDS.col("record1").equalTo(AliceBloomsDS.col("recordID")))
                .drop("recordID").withColumnRenamed("bloom","bloom1")
                .join(BobsBloomsDS,possibleMatchesDS.col("record2").equalTo(BobsBloomsDS.col("recordID")))
                .drop("recordID").withColumnRenamed("bloom","bloom2") ;

//        Dataset<Row> matches = possibleMatchesWithBloomsDS.filter((FilterFunction<Row>) mb::isMatch).drop("bloom1", "bloom2");

//        List<Row> matchesList = matches.collectAsList();

//        long matchesSize = matches.count();
        long tp =  possibleMatchesDS.filter((FilterFunction<Row>) row -> {
            return row.getString(0).substring(1).equals(row.getString(1).substring(1));
        }).count();
//        long fp = matchesSize - tp ;

        long commons = (long) (Conf.DB_SIZE * Conf.COMMON_RECORDS);
        long timer = (System.currentTimeMillis() - t0) / 1000;
        System.out.println("Execution time: " + timer + " seconds");
        System.out.println(tp);
//        System.out.println(matchesSize);
        System.out.println("Possible Recall (it may go above 1) : " + (double) tp / commons );
//        System.out.println("Possible Precision (it may go above 1) : " + (double) tp / matchesSize );

//        System.out.println(matches);
//        System.out.println("MATCHES");

        Scanner myscanner = new Scanner(System.in);
        myscanner.nextLine();
        myscanner.close();

        spark.close();




    }
}