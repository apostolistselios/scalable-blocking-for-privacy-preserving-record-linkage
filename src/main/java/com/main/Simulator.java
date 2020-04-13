package com.main;


import com.algorithms.BlockingAlgorithm;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Simulator {

    public static void main(String[] args) {


        //TODO  Implement with different inputs for Alice and Bob and merge them !!!

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
        SQLData db = new SQLData(conf);

        //For now only used for debugging
        db.startSQL();

        JavaRDD<List<String>> AlicesRDD = sc.parallelize(Alice_DB);
        JavaRDD<List<String>> BobsRDD = sc.parallelize(Bob_DB);

        ArrayList<JavaPairRDD<String, String>> AliceRDDs = new ArrayList<JavaPairRDD<String, String>>();
        for (int i = 1; i<=3; i++)
            AliceRDDs.add(BlockingAlgorithm.mapBlockingAttributes(AlicesRDD, i));

        ArrayList<JavaPairRDD<String, String>> BobRDDs = new ArrayList<JavaPairRDD<String, String>>();
        for (int i = 1; i<=3; i++)
            BobRDDs.add(BlockingAlgorithm.mapBlockingAttributes(BobsRDD, i));


        // classify for each
        // get the name_pairsRDD,last_nameRDD, etc. and classify it respectively with 1st reference set, 2nd, etc and add it into an ArrayList.
        ArrayList<JavaPairRDD<String, List<String>>> ClassifiedAlicesRDDs = new ArrayList<JavaPairRDD<String, List<String>>>();
        for (int i = 1; i<=3; i++)
            ClassifiedAlicesRDDs.add(BlockingAlgorithm.mapClassify(AliceRDDs.get(i-1),ReferenceSets.get(i-1) , String.valueOf(i)));

        ArrayList<JavaPairRDD<String, List<String>>> ClassifiedBobsRDDs = new ArrayList<JavaPairRDD<String, List<String>>>();

        for (int i = 1; i<=3; i++)
            ClassifiedBobsRDDs.add(BlockingAlgorithm.mapClassify(BobRDDs.get(i-1),ReferenceSets.get(i-1) , String.valueOf(i)));

        JavaPairRDD<String, Iterable<List<String>>> BobsRDDGrouped =  ClassifiedBobsRDDs.get(0).union(ClassifiedBobsRDDs.get(1).union(ClassifiedBobsRDDs.get(2))).groupByKey() ;
        JavaPairRDD<String, Iterable<List<String>>> AlicesRDDGrouped =  ClassifiedAlicesRDDs.get(0).union(ClassifiedAlicesRDDs.get(1).union(ClassifiedAlicesRDDs.get(2))).groupByKey() ;

        JavaPairRDD<String, String> BobsblocksRDD = BobsRDDGrouped.flatMapToPair(BlockingAlgorithm::combineBlocks);
        JavaPairRDD<String, String> AliceblocksRDD = AlicesRDDGrouped.flatMapToPair(BlockingAlgorithm::combineBlocks);

        // combine the 2 different databases Alices and Bob.
        JavaPairRDD<String, Tuple2<Iterable<String> , Iterable<String>>> CombinedBlocks= BobsblocksRDD.cogroup(AliceblocksRDD) ;

        //filter block which have only one source
        JavaPairRDD<String, Tuple2<Iterable<String> , Iterable<String>>> filteredBlocks = CombinedBlocks.filter(block -> {
            return block._2()._1().iterator().hasNext() && block._2()._2().iterator().hasNext() ;
        });

        //map blocks to <String , List<String> >
        JavaPairRDD<String,Iterable<String>> finalBlocks = filteredBlocks.mapValues(block -> {
            return Stream.concat(StreamSupport.stream(block._1().spliterator(),true),
                    StreamSupport.stream(block._2().spliterator(),true)).collect(Collectors.toList()) ;
        });
        

        finalBlocks.collect().forEach(System.out::println);

        Scanner myscanner = new Scanner(System.in);
        myscanner.nextLine();

        sc.close();

    }
}