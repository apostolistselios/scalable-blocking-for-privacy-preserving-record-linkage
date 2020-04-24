package com.simulator;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.io.BooleanWritable.Comparator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.algorithms.ReferenceSetBlocking;
import com.utils.Block;
import com.utils.BlockingAttribute;

public class Simulator {

    public static void main(String[] args) {

        //TODO  See if we can do this with Datasets
    	final int NUMBER_OF_BLOCKING_ATTRS = 3;

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<List<String>> Alice_DB = new ArrayList<>();
        List<List<String>> Bob_DB = new ArrayList<>();
        List<String> s1 = Arrays.asList("anthony", "lawrence", "victor", "zoe");
        List<String> s2 = Arrays.asList("alex", "dorothy", "jonathan", "naomi");
        List<String> s3 = Arrays.asList("alex", "john", "rhonda", "tristan");
        List<List<String>> ReferenceSets = Arrays.asList(s1,s2,s3);
        // Alice's data
        Alice_DB.add(Arrays.asList("a1", "nicholas", "smith", "madrid"));
        Alice_DB.add(Arrays.asList("a2", "ann", "cobb", "london"));

        // Bob's data
        Bob_DB.add(Arrays.asList("b1", "kevin", "anderson", "warsaw"));
        Bob_DB.add(Arrays.asList("b2", "anne", "cobb", "london"));

        SparkConf conf = new SparkConf().setAppName("startingSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        SQLData db = new SQLData(conf);
//
//        //For now only used for debugging
//        db.startSQL();

        JavaRDD<List<String>> AlicesRDD = sc.parallelize(Alice_DB);
        JavaRDD<List<String>> BobsRDD = sc.parallelize(Bob_DB);
        
        ReferenceSetBlocking rsb = new ReferenceSetBlocking();
         
        ArrayList<JavaPairRDD<String, String>> AliceRDDs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++)
            AliceRDDs.add(rsb.mapBlockingAttributes(AlicesRDD, i));

        ArrayList<JavaPairRDD<String, String>> BobRDDs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++)
            BobRDDs.add(rsb.mapBlockingAttributes(BobsRDD, i));
        
        // classify for each
        // get the name_pairsRDD, last_nameRDD, etc. and classify it respectively with 1st reference set, 2nd, etc and add it into an ArrayList.
        ArrayList<JavaPairRDD<String, BlockingAttribute>> ClassifiedAlicesRDDs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++)
            ClassifiedAlicesRDDs.add(rsb.classify(AliceRDDs.get(i-1), ReferenceSets.get(i-1), String.valueOf(i)));

        ArrayList<JavaPairRDD<String, BlockingAttribute>> ClassifiedBobsRDDs = new ArrayList<>();
        for (int i = 1; i <= NUMBER_OF_BLOCKING_ATTRS; i++)
            ClassifiedBobsRDDs.add(rsb.classify(BobRDDs.get(i-1), ReferenceSets.get(i-1), String.valueOf(i)));
        
//        for(JavaPairRDD<String, BlockingAttribute> rdd : ClassifiedAlicesRDDs) {
//        	for(Tuple2<String, BlockingAttribute> ba : rdd.collect()) {
//        		System.out.println(ba);
//        	}
//        }
//        
//        for(JavaPairRDD<String, BlockingAttribute> rdd : ClassifiedBobsRDDs) {
//        	for(Tuple2<String, BlockingAttribute> ba : rdd.collect()) {
//        		System.out.println(ba);
//        	}
//        }
       
        // data in rdds is like (recordID , BlockingAttribute(classID, score))
        JavaPairRDD<String, Iterable<BlockingAttribute>> BobsRDDGrouped =  ClassifiedBobsRDDs.get(0).union(ClassifiedBobsRDDs.get(1).union(ClassifiedBobsRDDs.get(2))).groupByKey() ;
        JavaPairRDD<String, Iterable<BlockingAttribute>> AlicesRDDGrouped =  ClassifiedAlicesRDDs.get(0).union(ClassifiedAlicesRDDs.get(1).union(ClassifiedAlicesRDDs.get(2))).groupByKey() ;
        
//        for(Tuple2<String, Iterable<BlockingAttribute>> ba: AlicesRDDGrouped.collect()) {
//        	System.out.println(ba);
//        }
        
        JavaPairRDD<String, BlockingAttribute> BobsblocksRDD = BobsRDDGrouped.flatMapToPair(rsb::combineBlocks);
        JavaPairRDD<String, BlockingAttribute> AliceblocksRDD = AlicesRDDGrouped.flatMapToPair(rsb::combineBlocks);
        
//        for(Tuple2<String, BlockingAttribute> ba: AliceblocksRDD.collect()) {
//        	System.out.println(ba);
//        }
        
        // combine the 2 different databases Alices and Bob.
        JavaPairRDD<String, BlockingAttribute> CombinedBlocks = BobsblocksRDD.union(AliceblocksRDD);
        
//        for(Tuple2<String, BlockingAttribute> ba: CombinedBlocks.collect()) {
//        	System.out.println(ba);
//        }
        
        JavaPairRDD<String, Iterable<BlockingAttribute>> groupedBlocks = CombinedBlocks.groupByKey();
        
//	    for(Tuple2<String, Iterable<BlockingAttribute>> ba: groupedBlocks.collect()) {
//	    	System.out.println(ba);
//	    }
        
////         filter the blocks with only one blocking attribute
//        JavaPairRDD<String, Iterable<BlockingAttribute>> filteredBlocks = groupedBlocks.filter(block -> {
//        	Iterator<BlockingAttribute> it = block._2().iterator();
//        	it.next();
//        	return it.hasNext();
//        });
	        
        JavaRDD<Block> blocks = groupedBlocks.map(block -> {
        	ArrayList<BlockingAttribute> baList = new ArrayList<>();
        	block._2().forEach(baList::add);
        	Block blockObj = new Block(block._1(), baList);
        	blockObj.calculateRank();
        	Collections.sort(blockObj.getBAList());
        	return blockObj;
        }).filter(block -> block.getBAList().size() >= 2);
        
//        JavaRDD<Block> filteredBlocks = blocks.filter(block -> block.getBAList().size() >= 2);
        
        System.out.println("BLOCKS");
        for(Block block : blocks.collect()) {
        	System.out.println(block);
        }

//        //map blocks to <String , List<String> >
//        JavaPairRDD<String,Iterable<Tuple2<String, Integer>>> finalBlocks = filteredBlocks.mapValues(block -> Stream.concat(StreamSupport.stream(block._1().spliterator(),true),
//                StreamSupport.stream(block._2().spliterator(),true)).sorted(Comparator.comparingInt(Tuple2::_2)).collect(Collectors.toList()));
// 
//        JavaPairRDD<String,Integer> predictedMatchesRDD = finalBlocks.flatMapToPair(block -> {
//
//                List<Tuple2<String,Integer>> records = new ArrayList<>() ;
//                Iterator<Tuple2<String, Integer>>  it = block._2().iterator() ;
//
//                Tuple2<String, Integer> currentBA = it.next();
//                String record ;
//                while (true) {
//                    Tuple2<String, Integer> nextBA;
//
//                    if (it.hasNext()) {
//                        nextBA = it.next() ;
//                        record = currentBA._1().concat(nextBA._1()) ;
//                        records.add(new Tuple2<>(record,1)) ;
//                        currentBA = nextBA ;
//                    } else
//                        break;
//                 }
//                return records.iterator();
//        });
//
//        JavaPairRDD<String, Integer> matchesCount = predictedMatchesRDD.reduceByKey(Integer::sum) ;
//
//        matchesCount.collect().forEach(System.out::println);

        Scanner myscanner = new Scanner(System.in);
        myscanner.nextLine();
        myscanner.close();
        
        sc.close();
    }
}