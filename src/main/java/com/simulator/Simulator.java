package com.simulator;


import com.algorithms.MetaBlocking;
import com.algorithms.ReferenceSetBlocking;
import com.database.SQLData;
import com.utils.Block;
import com.utils.BlockingAttribute;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.col;

public class Simulator {

    public static void main(String[] args) {

        //TODO  meta blocking
    	final int NUMBER_OF_BLOCKING_ATTRS = 3;

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("JavaRDD").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        SQLData db = new SQLData(spark, "1k");

        JavaRDD<List<String>> AlicesRDD = db.getAlice().toJavaRDD().map(row -> {
            List<String> list = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                list.add(row.getString(i));
            }
            return list;
        });

        JavaRDD<List<String>> BobsRDD = db.getBob().toJavaRDD().map(row -> {
            List<String> list = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                list.add(row.getString(i));
            }
            return list;
        });

        Dataset<Row> ReferenceSets = db.getReference_set();

        System.out.println("Data form DB loaded");
        long t0 = System.currentTimeMillis();

        /*  data in BobsRDD is like
           [AT24345, ABEDE, ELILA, BURLINcTON]
           [AA181290, ACO0TA, BARBRAA, BURLNGTON]
         */

        ReferenceSetBlocking rsb = new ReferenceSetBlocking();
         
        ArrayList<JavaPairRDD<String, String>> AliceRDDs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++)
            AliceRDDs.add(rsb.mapBlockingAttributes(AlicesRDD, i));

        ArrayList<JavaPairRDD<String, String>> BobRDDs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++)
            BobRDDs.add(rsb.mapBlockingAttributes(BobsRDD, i));

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
        JavaPairRDD<String, Iterable<BlockingAttribute>> BobsRDDGrouped =  ClassifiedBobsRDDs.get(0).union(ClassifiedBobsRDDs.get(1).union(ClassifiedBobsRDDs.get(2))).groupByKey() ;
        JavaPairRDD<String, Iterable<BlockingAttribute>> AlicesRDDGrouped =  ClassifiedAlicesRDDs.get(0).union(ClassifiedAlicesRDDs.get(1).union(ClassifiedAlicesRDDs.get(2))).groupByKey() ;

        /*
        data in BobsRDD for grouped and classified records  is like
        (AA181290,[BA(S1.2,null,13), BA(S2.1,null,14), BA(S3.1,null,15)])
        (AT24345,[BA(S1.2,null,13), BA(S2.1,null,14), BA(S3.1,null,15)])
         */

        JavaPairRDD<String, BlockingAttribute> BobsblocksRDD = BobsRDDGrouped.flatMapToPair(rsb::combineBlocks);
        JavaPairRDD<String, BlockingAttribute> AliceblocksRDD = AlicesRDDGrouped.flatMapToPair(rsb::combineBlocks);
        System.out.println("1");
        BobsblocksRDD.collect().forEach(System.out::println);
         /*
         data in BobsblocksRDD  is like
        (S1.2-S2.1,BA(S1.2,AA181290,13))
        (S2.1-S3.1,BA(S2.1,AA181290,14))
        (S3.1-S1.2,BA(S3.1,AA181290,15))
        (S1.2-S2.1,BA(S1.2,AT24345,13))
        (S2.1-S3.1,BA(S2.1,AT24345,14))
        (S3.1-S1.2,BA(S3.1,AT24345,15))
         */

        // combine the 2 different databases Alices and Bob.
        //TODO Make sure that blocks have records from both databases
        JavaPairRDD<String, BlockingAttribute> CombinedBlocks = BobsblocksRDD.union(AliceblocksRDD);

        // Data in CombinedBlocks are like  last BobsblocksRDD representation but includes records from both dbs


        JavaPairRDD<String, Iterable<BlockingAttribute>> groupedBlocks = CombinedBlocks.groupByKey();
        /* Data in groupedBlocks is like
        (S3.1-S1.2,[BA(S3.1,AA181290,15), BA(S3.1,AT24345,15), BA(S3.1,AA181290,15), BA(S3.1,AT24345,15)])
        (S1.2-S2.1,[BA(S1.2,AA181290,13), BA(S1.2,AT24345,13), BA(S1.2,AA181290,13), BA(S1.2,AT24345,13)])
        (S2.1-S3.1,[BA(S2.1,AA181290,14), BA(S2.1,AT24345,14), BA(S2.1,AA181290,14), BA(S2.1,AT24345,15)])
        */


        JavaRDD<Block> blocks = groupedBlocks.map(block -> {
        	ArrayList<BlockingAttribute> baList = new ArrayList<>();
        	block._2().forEach(baList::add);
        	Block blockObj = new Block(block._1(), baList);
        	blockObj.calculateRank();
        	Collections.sort(blockObj.getBAList());
        	return blockObj;
        }).filter(block -> block.getBAList().size() >= 2);


        long timer = (System.currentTimeMillis() - t0) / 1000;
        
        System.out.println("BLOCKS");
        for(Block block : blocks.collect()) {
        	System.out.println(block);
        }
        System.out.println("Execution time: " + timer + " seconds");

        MetaBlocking mb = new MetaBlocking();
        
        JavaPairRDD<String, Integer> matches = mb.predict(blocks).reduceByKey(Integer::sum);

        // check if we have matches
//        JavaPairRDD<String, Integer> matches = mb.predict(blocks).reduceByKey(Integer::sum).filter(match ->{
//            int sep = match._1().indexOf("-") ;
//            String rec1 = match._1().substring(0,sep);
//            String rec2 = match._1().substring(sep+1);
//            return rec1.equals(rec2);
//        } );
        System.out.println("MATCHES");
        for(Tuple2<String,Integer> match : matches.collect()) {
        	System.out.println(match);
        }

        Scanner myscanner = new Scanner(System.in);
        myscanner.nextLine();
        myscanner.close();
        
        spark.close();
    }
}