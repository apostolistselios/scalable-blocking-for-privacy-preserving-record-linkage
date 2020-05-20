package com.simulator;


import com.algorithms.ReferenceSetBlocking;
import com.database.SQLData;
import com.utils.Bigrams;
import com.utils.Block;
import com.utils.BlockElement;
import com.utils.BlockingAttribute;
import info.debatty.java.stringsimilarity.SorensenDice;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.sketch.BloomFilter;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.spark.sql.functions.col;

public class Simulator {

    public static void main(String[] args) {

        //TODO  meta blocking
        final int NUMBER_OF_BLOCKING_ATTRS = 3;

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("JavaRDD").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        SQLData db = new SQLData(spark, "1k");

        Dataset<Row> AliceDS = db.getAlice();

        JavaRDD<List<String>> AlicesRDD = AliceDS.toJavaRDD().map(row -> {
            List<String> list = new ArrayList<>();
            // change id to include source
            list.add("A" + row.getString(0));
            for (int i = 1; i < row.size(); i++) {
                list.add(row.getString(i));
            }
            return list;
        });

        Dataset<Row> BobsDS = db.getBob();
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
        (S2.1-S3.1,BA(S2.1,AA181290,14))
        (S3.1-S1.2,BA(S3.1,AA181290,15))
        (S1.2-S2.1,BA(S1.2,AT24345,13))
        (S2.1-S3.1,BA(S2.1,AT24345,14))
        (S3.1-S1.2,BA(S3.1,AT24345,15))
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
        (S1.2-S2.1,([BA(S1.2,AA181290,13), BA(S1.2,AT24345,13)],[BA(S1.2,AA181290,13), BA(S1.2,AT24345,13)]))
        (S2.1-S3.1,([BA(S2.1,AA181290,14), BA(S2.1,AT24345,14)],[BA(S2.1,AA181290,14), BA(S2.1,AT24345,15)]))
        (S3.1-S1.2,([BA(S3.1,AA181290,15), BA(S3.1,AT24345,15)],[BA(S3.1,AA181290,15), BA(S3.1,AT24345,15)]))
        (S1.2-S2.1,([BA(S1.2,AA181290,13), BA(S1.2,AT24345,13)],[BA(S1.2,AA181290,13), BA(S1.2,AT24345,13)]))
        (S2.1-S3.1,([BA(S2.1,AA181290,14), BA(S2.1,AT24345,14)],[BA(S2.1,AA181290,14), BA(S2.1,AT24345,15)]))
        */
        // Data in CombinedBlocks are like  last BobsblocksRDD representation but includes records from both dbs

        JavaRDD<Block> blocks = filteredBlocks.map(block -> {
            ArrayList<BlockElement> baList = (ArrayList<BlockElement>) Stream.concat(StreamSupport.stream(block._2()._1().spliterator(), true),
                    StreamSupport.stream(block._2()._2().spliterator(), true)).collect(Collectors.toList());

            Block blockObj = new Block(block._1(), baList);
            blockObj.calculateRank();
            Collections.sort(blockObj.getBAList());
            return blockObj;
        });

        /* Data in blocks is like
        [BLOCK: S3.1-S1.2 - Rank: 60 - [BA(S3.1,BAT24345,15), BA(S3.1,BAA181290,15), BA(S3.1,AAA181290,15), BA(S3.1,AAT24345,15)]]
        [BLOCK: S1.2-S2.1 - Rank: 52 - [BA(S1.2,BAT24345,13), BA(S1.2,BAA181290,13), BA(S1.2,AAA181290,13), BA(S1.2,AAT24345,13)]]
        [BLOCK: S2.1-S3.1 - Rank: 57 - [BA(S2.1,BAT24345,14), BA(S2.1,BAA181290,14), BA(S2.1,AAA181290,14), BA(S2.1,AAT24345,15)]]
        */

        long timer = (System.currentTimeMillis() - t0) / 1000;

//        System.out.println("BLOCKS");
//        for(Block block : blocks.collect()) {
//        	System.out.println(block);
//        }
        System.out.println("Execution time: " + timer + " seconds");

        List<Block> blockList = blocks.collect();
        ArrayList<String> matches = new ArrayList<>();

        int window = 2;
        for (Block block : blockList) {
            ArrayList<BlockElement> baList = block.getBAList();

            for (int i = 1; i < baList.size(); i++) {
                String record1 = baList.get(i).getRecordID();
                for (int j = i - 1; j >= i - window && j >= 0; j--) {
                    String record2 = baList.get(j).getRecordID();
                    char firstcharOfrecord1 = record1.charAt(0);
                    char firstcharOfrecord2 = record2.charAt(0);

                    if (firstcharOfrecord1 != firstcharOfrecord2) {

                        Row record1Attributes;
                        Row record2Attributes;
                        if (firstcharOfrecord1 == 'A') {
                            record1Attributes = AliceDS.filter(AliceDS.col("id").equalTo(record1.substring(1))).first();
                            record2Attributes = BobsDS.filter(BobsDS.col("id").equalTo(record2.substring(1))).first();
                        } else {
                            record1Attributes = BobsDS.filter(BobsDS.col("id").equalTo(record1.substring(1))).first();
                            record2Attributes = AliceDS.filter(AliceDS.col("id").equalTo(record2.substring(1))).first();
                        }

                        // TODO  Bloom Filters 
                        List<List<String>> BigramsAttributesRecord1 = new ArrayList<>();
                        List<List<String>> BigramsAttributesRecord2 = new ArrayList<>();
//                        List<BloomFilter> BloomFiltersRecord1 = new ArrayList<BloomFilter>();
//                        List<BloomFilter> BloomFiltersRecord2 = new ArrayList<BloomFilter>();
                        boolean match = false;
                        for (int k = 1; k < record1Attributes.size(); k++) {
                            String temp = record1Attributes.getString(k);
                            String temp2 = record2Attributes.getString(k);
                            BigramsAttributesRecord1.add(Bigrams.ngrams(2, temp));
                            BigramsAttributesRecord2.add(Bigrams.ngrams(2, temp2));
                            BloomFilter bloom1 = BloomFilter.create(BigramsAttributesRecord1.get(k - 1).size());
                            BloomFilter bloom2 = BloomFilter.create(BigramsAttributesRecord2.get(k - 1).size());
                            for (String bigram : BigramsAttributesRecord1.get(k - 1)) {
                                bloom1.putString(bigram);
                            }
                            for (String bigram : BigramsAttributesRecord2.get(k - 1)) {
                                bloom2.putString(bigram);
                            }
                            ByteArrayOutputStream bloom1Byte = new ByteArrayOutputStream();
                            ByteArrayOutputStream bloom2Byte = new ByteArrayOutputStream();
                            try {
                                bloom1.writeTo(bloom1Byte);
                                bloom2.writeTo(bloom2Byte);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            String DiceRecord1 = Arrays.toString(bloom1Byte.toByteArray());
                            String DiceRecord2 = Arrays.toString(bloom2Byte.toByteArray());
                            SorensenDice Similiarity = new SorensenDice();
                            double THRESHOLD = 0.5;
                            if (Similiarity.similarity(DiceRecord1, DiceRecord2) < THRESHOLD) {
                                match = false;
                                break;
                            } else {
                                match = true;
                            }
                            // TODO HASHMAP FOR RECORDS MATCHES BLOCK.
                            // WINDOW SIZE IF MATCH JUMP 2.
                            if (match) {
                                matches.add(record1Attributes.getString(0) + " " + record2Attributes.getString(0) + " MATCH.");
                            }
                        }


                    }
                }
            }

            matches.forEach(System.out::println);
            //MetaBlocking mb = new MetaBlocking();
            //JavaPairRDD<String, Integer> matches = mb.predict(blocks,2, AliceDS,BobsDS).reduceByKey(Integer::sum) ;
            // check if we have matches
//        JavaPairRDD<String, Integer> matches = mb.predict(blocks).reduceByKey(Integer::sum).filter(match ->{
//            int sep = match._1().indexOf("-") ;
//            String rec1 = match._1().substring(0,sep);
//            String rec2 = match._1().substring(sep+1);
//            return rec1.equals(rec2);
//        } );
//        System.out.println(matches);
//        System.out.println("MATCHES");
//        matches.collect().forEach(System.out::println);

            Scanner myscanner = new Scanner(System.in);
            myscanner.nextLine();
            myscanner.close();

            spark.close();
        }


    }
}