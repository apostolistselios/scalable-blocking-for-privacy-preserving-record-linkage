package com.simulator;

import com.algorithms.ReferenceSetBlocking;
import com.utils.BlockingAttribute;
import com.utils.Record;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class DatasetSimulator {



    public static void main(String[] args) {

        final int NUMBER_OF_BLOCKING_ATTRS = 3;

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("Dataset").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        List<String> s1 = Arrays.asList("anthony", "lawrence", "victor", "zoe");
        List<String> s2 = Arrays.asList("alex", "dorothy", "jonathan", "naomi");
        List<String> s3 = Arrays.asList("alex", "john", "rhonda", "tristan");
        List<List<String>> ReferenceSets = Arrays.asList(s1,s2,s3);

        Dataset<Row> Alice_DS = spark.createDataFrame(Arrays.asList(
                new Record("a1", "nicholas", "smith", "madrid"),
                new Record("a2", "ann", "cobb", "london")
        ), Record.class);


        Dataset<Row> Bob_DS = spark.createDataFrame(Arrays.asList(
                new Record("b1", "kevin", "anderson", "warsaw"),
                new Record("b2", "anne", "cobb", "london")
        ), Record.class);
        /*  data in Bob_DS is like
            +------+---+-----+--------+
            |  city| id| name| surname|
            +------+---+-----+--------+
            |warsaw| b1|kevin|anderson|
            |london| b2| anne|    cobb|
            +------+---+-----+--------+
         */

        ReferenceSetBlocking rsb = new ReferenceSetBlocking();

        // distribute blocking attributes in different datasets
        ArrayList<Dataset<Row>> AliceDSs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++)
            AliceDSs.add(rsb.mapBlockingAttributes(Alice_DS, i));

        ArrayList<Dataset<Row>> BobDSs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++)
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
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++)
            ClassifiedAlicesDSs.add(rsb.classify(AliceDSs.get(i-1), ReferenceSets.get(i-1), String.valueOf(i)));

        ArrayList<Dataset<BlockingAttribute>> ClassifiedBobsDSs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++)
            ClassifiedBobsDSs.add(rsb.classify(BobDSs.get(i-1), ReferenceSets.get(i-1), String.valueOf(i)));
        /* data in ds is like
         example of classification for name as blocking attribute for Bob's db
            +-------+--------+-----+
            |classID|recordID|score|
            +-------+--------+-----+
            |   S1.1|      b1|    6|
            |   S1.1|      b2|    4|
            +-------+--------+-----+

         */


        Dataset<Row> BobsDSGrouped =  ClassifiedBobsDSs.get(0).union(ClassifiedBobsDSs.get(1).union(ClassifiedBobsDSs.get(2)))
                .groupBy("recordID").agg(functions.collect_list("classID"),functions.collect_list("score"));
        Dataset<Row> AlicesDSGrouped =  ClassifiedAlicesDSs.get(0).union(ClassifiedAlicesDSs.get(1).union(ClassifiedAlicesDSs.get(2)))
                .groupBy("recordID").agg(functions.collect_list("classID"),functions.collect_list("score"));

        /* data in ds is like
        example of grouping for Bob's db
        +--------+---------------------+-------------------+
        |recordID|collect_list(classID)|collect_list(score)|
        +--------+---------------------+-------------------+
        |      b2|   [S3.2, S1.1, S2.1]|          [4, 4, 4]|
        |      b1|   [S3.4, S2.1, S1.1]|          [5, 6, 6]|
        +--------+---------------------+-------------------+
        */
        BobsDSGrouped.show();




        Scanner myscanner = new Scanner(System.in);
        myscanner.nextLine();
        myscanner.close();

        spark.close();





    }
}
