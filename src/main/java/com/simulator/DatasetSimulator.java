package com.simulator;

import com.algorithms.ReferenceSetBlocking;
import com.database.SQLData;
import com.utils.Timer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class DatasetSimulator {

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("Dataset").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        SQLData db = new SQLData(spark);

        Dataset<Row> Alice_DS = db.getAlice();
        Dataset<Row> Bob_DS = db.getBob();
        Dataset<Row> ReferenceSets = db.getReferenceSet();

        /*  data in Bob_DS is like
            +------+---+-----+--------+
            |  city| id| name| surname|
            +------+---+-----+--------+
            |warsaw| b1|kevin|anderson|
            |london| b2| anne|    cobb|
            +------+---+-----+--------+
         */
        Timer.start();

        // This does not work properly for now
        ReferenceSetBlocking.blocking(Alice_DS, Bob_DS, ReferenceSets);

        //TODO meta-blocking

        printResult(Alice_DS, Bob_DS);

        Scanner myscanner = new Scanner(System.in);
        myscanner.nextLine();
        myscanner.close();

        spark.close();
    }

    private static void printResult(Dataset<Row> bobsBlocksDS, Dataset<Row> alicesBlocksDS) {
        bobsBlocksDS.show();
        alicesBlocksDS.show();

        System.out.println("Execution time: " + Timer.stop() + " seconds");
    }
}
