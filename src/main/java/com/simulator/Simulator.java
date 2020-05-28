package com.simulator;


import com.algorithms.MetaBlocking;
import com.algorithms.ReferenceSetBlocking;
import com.database.SQLData;
import com.utils.Block;
import com.utils.BlockElement;
import com.utils.BlockingAttribute;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.apache.spark.sql.functions.col;

public class Simulator {

    public static void main(String[] args) {

        //TODO  meta blocking
        final int NUMBER_OF_BLOCKING_ATTRS = 3;

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("JavaRDD")
                .set("spark.executor.memory", "5g");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        SQLData db = new SQLData(spark, "50k");

        Dataset<Row> AlicesDS = db.getAlice() ;
        AlicesDS.persist(StorageLevel.MEMORY_ONLY());
        JavaRDD<List<String>> AlicesRDD = AlicesDS.toJavaRDD().map(row -> {
            List<String> list = new ArrayList<>();
            // change id to include source
            list.add("A" + row.getString(0));
            for (int i = 1; i < row.size(); i++) {
                list.add(row.getString(i));
            }
            return list;
        });

        Dataset<Row> BobsDS = db.getAlice() ;
        BobsDS.persist(StorageLevel.MEMORY_ONLY());
        JavaRDD<List<String>> BobsRDD = BobsDS.toJavaRDD().map(row -> {
            List<String> list = new ArrayList<>();
            // change id to include source
            list.add("B" + row.getString(0));
            for (int i = 1; i < row.size(); i++) {
                list.add(row.getString(i));
            }
            return list;
        });


        Dataset<Row> ReferenceSets = db.getReferenceSet();

        System.out.println("Data from DB loaded");
        long t0 = System.currentTimeMillis();

        /*  data in BobsRDD is like
           [AT24345, ABEDE, ELILA, BURLINcTON]
           [AA181290, ACO0TA, BARBRAA, BURLNGTON]
         */

        ReferenceSetBlocking rsb = new ReferenceSetBlocking();

        ArrayList<JavaPairRDD<String, String>> AliceRDDs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++) {
            // copy i to final variable just to pass it as parameter
            int finalI = i;
            AliceRDDs.add(AlicesRDD.mapToPair(record -> rsb.mapBlockingAttributes(record, finalI)));
        }


        ArrayList<JavaPairRDD<String, String>> BobRDDs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++) {
            // copy i to final variable just to pass it as parameter
            int finalI = i;
            BobRDDs.add(BobsRDD.mapToPair(record -> rsb.mapBlockingAttributes(record, finalI)));
        }

        /*  data in BobsRDD for attribute name is like
           (AT24345,ABEDE)
           (AA181290,ACO0TA)
         */


        // classify for each
        // get the name_pairsRDD, last_nameRDD, etc. and classify it respectively with 1st reference set, 2nd, etc and add it into an ArrayList.
        ArrayList<JavaPairRDD<String, BlockingAttribute>> ClassifiedAlicesRDDs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++) {
            ClassifiedAlicesRDDs.add(rsb.classify(
                    AliceRDDs.get(i - 1)
                    , ReferenceSets.select(col("_c" + i)).as(Encoders.STRING()).collectAsList()
                    , String.valueOf(i)));
        }

        ArrayList<JavaPairRDD<String, BlockingAttribute>> ClassifiedBobsRDDs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++) {
            ClassifiedBobsRDDs.add(rsb.classify(
                    BobRDDs.get(i - 1)
                    , ReferenceSets.select(col("_c" + i)).as(Encoders.STRING()).collectAsList()
                    , String.valueOf(i)));
        }
        /*
        data in BobsRDD for classified attribute name is like
        (AT24345,BA(S1.2,null,13))
        (AA181290,BA(S1.2,null,13))
         */


        // data in rdds is like (recordID , BlockingAttribute(classID, score))
        JavaPairRDD<String, Iterable<BlockingAttribute>> BobsRDDGrouped = ClassifiedBobsRDDs.get(0).union(ClassifiedBobsRDDs.get(1).union(ClassifiedBobsRDDs.get(2))).groupByKey();
        JavaPairRDD<String, Iterable<BlockingAttribute>> AlicesRDDGrouped = ClassifiedAlicesRDDs.get(0).union(ClassifiedAlicesRDDs.get(1).union(ClassifiedAlicesRDDs.get(2))).groupByKey();

        /*
        data in BobsRDD for grouped and classified records  is like
        (AA181290,[BA(S1.2,null,13), BA(S2.1,null,14), BA(S3.1,null,15)])
        (AT24345,[BA(S1.2,null,13), BA(S2.1,null,14), BA(S3.1,null,15)])
         */

        JavaPairRDD<String, BlockElement> BobsblocksRDD = BobsRDDGrouped.flatMapToPair(rsb::combineBlocks);
        JavaPairRDD<String, BlockElement> AliceblocksRDD = AlicesRDDGrouped.flatMapToPair(rsb::combineBlocks);

         /*
         data in BobsblocksRDD  is like
        (S1.2-S2.1,BA(S1.2,AA181290,13))
         */

        // combine the 2 different databases Alices and Bob.
        //TODO Make sure that blocks have records from both databases

        // combine the 2 different databases Alices and Bob.
        JavaPairRDD<String, Tuple2<Iterable<BlockElement>, Iterable<BlockElement>>> CombinedBlocks = BobsblocksRDD.cogroup(AliceblocksRDD);

        //filter block which have only one source
        JavaPairRDD<String, Tuple2<Iterable<BlockElement>, Iterable<BlockElement>>> filteredBlocks = CombinedBlocks.filter(block -> {
            return block._2()._1().iterator().hasNext() && block._2()._2().iterator().hasNext();
        });

        /* Data in filteredBlocks is like
        (S3.1-S1.2,([BA(S3.1,AA181290,15), BA(S3.1,AT24345,15)],[BA(S3.1,AA181290,15), BA(S3.1,AT24345,15)]))
        */
        // Data in CombinedBlocks are like  last BobsblocksRDD representation but includes records from both dbs

        JavaRDD<Block> blocks = filteredBlocks.map(rsb::sortBlockElements);

        /* Data in blocks is like
        [BLOCK: S3.1-S1.2 - Rank: 60 - [BA(S3.1,BAT24345,15), BA(S3.1,BAA181290,15), BA(S3.1,AAA181290,15), BA(S3.1,AAT24345,15)]]
        */
        MetaBlocking mb = new MetaBlocking();

        // Create datasets with records' blooms filters
        //define the schema for blooms dataset
        StructType bloomsFilterSchema = new StructType();
        bloomsFilterSchema = bloomsFilterSchema.add("recordID", DataTypes.StringType, false);
        bloomsFilterSchema = bloomsFilterSchema.add("bloom", DataTypes.BinaryType , false);
        ExpressionEncoder<Row> bloomsFilterEncoder = RowEncoder.apply(bloomsFilterSchema);

        Dataset<Row> AliceBloomsDS = spark.createDataset(AlicesRDD.map(mb::createBloomFilters).rdd(),bloomsFilterEncoder) ;
        Dataset<Row> BobsBloomsDS = spark.createDataset(BobsRDD.map(mb::createBloomFilters).rdd(),bloomsFilterEncoder) ;



        //define the schema for possible matches dataset
        StructType possiblesMatchesSchema = new StructType();
        // this is the column for records from Alice's database
        possiblesMatchesSchema = possiblesMatchesSchema.add("record1", DataTypes.StringType, false);
        // this is the column for records from Bob;s database
        possiblesMatchesSchema = possiblesMatchesSchema.add("record2", DataTypes.StringType, false);
        ExpressionEncoder<Row> possibleMatchesEncoder = RowEncoder.apply(possiblesMatchesSchema);

        int window = 20;

        // create possibleMatchesDS that contains only unique rows
        Dataset<Row> possibleMatchesDS = spark.createDataset(blocks.flatMap(block -> mb.createPossibleMatches(block,window)).rdd(),
                possibleMatchesEncoder).distinct() ;

        Dataset<Row> possibleMatchesWithBloomsDS = possibleMatchesDS.join(AliceBloomsDS,
                possibleMatchesDS.col("record1").equalTo(AliceBloomsDS.col("recordID")))
                .drop("recordID").withColumnRenamed("bloom","bloom1")
                .join(BobsBloomsDS,possibleMatchesDS.col("record2").equalTo(BobsBloomsDS.col("recordID")))
                .drop("recordID").withColumnRenamed("bloom","bloom2") ;



        double THRESHOLD = 0.7 ;
        Dataset<Row> matches = possibleMatchesWithBloomsDS.filter((FilterFunction<Row>) row -> mb.isMatch(row,THRESHOLD)) ;



        long matchesSize = matches.count();
        long tp =   matches.filter((FilterFunction<Row>) row -> {
            return row.getString(0).substring(1).equals(row.getString(1).substring(1));
        }).count();
        long fp = matchesSize - tp ;

        long commons = (long) 1000;
        long timer = (System.currentTimeMillis() - t0) / 1000;
        System.out.println("Execution time: " + timer + " seconds");
        System.out.println(tp);
        System.out.println(matchesSize);
        System.out.println("Possible Recall ( it may go above 1 ) : " + (double) tp / commons );
        System.out.println("Possible Precision ( it may go above 1 ) : " + (double) tp / matchesSize );

//        System.out.println(matches);
//        System.out.println("MATCHES");

        Scanner myscanner = new Scanner(System.in);
        myscanner.nextLine();
        myscanner.close();

        spark.close();




    }

}