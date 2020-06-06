package com.simulator;


import com.algorithms.MetaBlocking;
import com.algorithms.ReferenceSetBlocking;
import com.database.SQLData;
import com.model.Block;
import com.utils.Conf;
import com.utils.Encoders;
import com.utils.Timer;
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

        Timer.start();

        /*
         * data in BobsRDD is like
         * [AT24345, ABEDE, ELILA, BURLINcTON]
         * [AA181290, ACO0TA, BARBRAA, BURLNGTON]
         */

        JavaRDD<Block> blocks = ReferenceSetBlocking.blocking(AlicesRDD, BobsRDD, ReferenceSets);

        Dataset<Row> matches = MetaBlocking.metaBocking(spark, AlicesRDD, BobsRDD, blocks);

        printResult(matches);

//        System.out.println(matches);
//        System.out.println("MATCHES");

        Scanner myscanner = new Scanner(System.in);
        myscanner.nextLine();
        myscanner.close();

        spark.close();
    }

    private static void printResult(Dataset<Row> matches) {
//        long matchesSize = matches.count();
        long tp =  matches.filter((FilterFunction<Row>) row -> {
            return row.getString(0).substring(1).equals(row.getString(1).substring(1));
        }).count();
//        long fp = matchesSize - tp ;

        System.out.println("Execution time: " + Timer.stop() + " seconds");
        long commons = (long) (Conf.DB_SIZE * Conf.COMMON_RECORDS);
        System.out.println(tp);
//        System.out.println(matchesSize);
        System.out.println("Possible Recall (it may go above 1) : " + (double) tp / commons );
//        System.out.println("Possible Precision (it may go above 1) : " + (double) tp / matchesSize );
    }
}