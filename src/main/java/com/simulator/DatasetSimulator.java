package com.simulator;

import com.algorithms.ReferenceSetBlocking;
import com.database.SQLData;
import com.utils.BlockingAttribute;
import com.utils.Conf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Scanner;

import static org.apache.spark.sql.functions.col;

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

        System.out.println("Data form DB loaded");
        long t0 = System.currentTimeMillis();

        ReferenceSetBlocking rsb = new ReferenceSetBlocking();

        // distribute blocking attributes in different datasets
        ArrayList<Dataset<Row>> AliceDSs = new ArrayList<>();
        for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++)
            AliceDSs.add(rsb.mapBlockingAttributes(Alice_DS, i));

        ArrayList<Dataset<Row>> BobDSs = new ArrayList<>();
        for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++)
            BobDSs.add(rsb.mapBlockingAttributes(Bob_DS, i));
        /* data in ds is like
        example of Bob's db for attribute name
        +---+-----+
        | id| name|
        +---+-----+
        | b1|kevin|
        | b2| anne|
        +---+-----+

        */

        // classify for each
        // classify respectively for every blocking attribute with 1st reference set, 2nd, etc and add it into an ArrayList.
        ArrayList<Dataset<BlockingAttribute>> ClassifiedAlicesDSs = new ArrayList<>();
        for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++) {
            ClassifiedAlicesDSs.add(rsb.classify(
                    AliceDSs.get(i - 1)
                    , ReferenceSets.select(col("col" + i)).as(Encoders.STRING()).collectAsList()
                    , String.valueOf(i)));
        }

        ArrayList<Dataset<BlockingAttribute>> ClassifiedBobsDSs = new ArrayList<>();
        for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++) {
            ClassifiedBobsDSs.add(rsb.classify(
                    BobDSs.get(i - 1)
                    , ReferenceSets.select(col("col" + i)).as(Encoders.STRING()).collectAsList()
                    , String.valueOf(i)));
        }
        /* data in ds is like
         example of classification for name as blocking attribute for Bob's db
            +-------+--------+-----+
            |classID|recordID|score|
            +-------+--------+-----+
            |   S1.1|      b1|    6|
            |   S1.1|      b2|    4|
            +-------+--------+-----+
         */

        //define the schema for Blocks dataset
        StructType schema = new StructType();
        schema = schema.add("blockID", DataTypes.StringType, false);
        schema = schema.add("record", DataTypes.createMapType(DataTypes.StringType,DataTypes.IntegerType), false);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);

        Dataset<BlockingAttribute> tempDS =  ClassifiedBobsDSs.get(0);
        for (int i = 1; i < Conf.NUM_OF_BLOCKING_ATTRS; i++){
            tempDS = tempDS.union(ClassifiedBobsDSs.get(i));
        }

        Dataset<Row> BobsBlocksDS = tempDS
                .groupBy("recordID").agg(functions.collect_list("classID"),functions.collect_list("score"))
                .flatMap(rsb::combineBlocksDS,encoder);

        tempDS =  ClassifiedAlicesDSs.get(0);
        for (int i = 1; i < Conf.NUM_OF_BLOCKING_ATTRS; i++){
            tempDS = tempDS.union(ClassifiedAlicesDSs.get(i));
        }

        Dataset<Row> AlicesBlocksDS = tempDS
                .groupBy("recordID").agg(functions.collect_list("classID"),functions.collect_list("score"))
                .flatMap(rsb::combineBlocksDS,encoder);
        /* After groupBy data in ds is like : example of grouping for Bob's db
                 +--------+---------------------+-------------------+
                |recordID|collect_list(classID)|collect_list(score)|
                +--------+---------------------+-------------------+
                |      b2|   [S3.2, S1.1, S2.1]|          [4, 4, 4]|
                |      b1|   [S3.4, S2.1, S1.1]|          [5, 6, 6]|
                +--------+---------------------+-------------------+
           After flatmap data in ds is like : example for Bob's Blocks
                +---------+----------+
                |  blockID|    record|
                +---------+----------+
                |S1.1-S2.1| [b2 -> 8]|
                |S1.1-S3.2| [b2 -> 8]|
                |S2.1-S3.2| [b2 -> 8]|
                |S1.1-S2.1|[b1 -> 12]|
                |S1.1-S3.4|[b1 -> 11]|
                |S2.1-S3.4|[b1 -> 11]|
                +---------+----------+
        */

        long timer = (System.currentTimeMillis() - t0) / 1000;

        BobsBlocksDS.show();
        AlicesBlocksDS.show();
        //TODO GroupBy blockID , sort on records of every block and meta-blocking

        System.out.println("Execution time: " + timer + " seconds");

        Scanner myscanner = new Scanner(System.in);
        myscanner.nextLine();
        myscanner.close();

        spark.close();





    }
}
