package com.main;


import com.algorithms.BlockingAlgorithm;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Simulator {

    public static void main(String[] args) {


        //TODO  See if we can do this with Datasets

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

        ArrayList<JavaPairRDD<String, String>> AliceRDDs = new ArrayList<JavaPairRDD<String, String>>();
        for (int i = 1; i<=3; i++)
            AliceRDDs.add(BlockingAlgorithm.mapBlockingAttributes(AlicesRDD, i));

        ArrayList<JavaPairRDD<String, String>> BobRDDs = new ArrayList<JavaPairRDD<String, String>>();
        for (int i = 1; i<=3; i++)
            BobRDDs.add(BlockingAlgorithm.mapBlockingAttributes(BobsRDD, i));


        // classify for each
        // get the name_pairsRDD,last_nameRDD, etc. and classify it respectively with 1st reference set, 2nd, etc and add it into an ArrayList.
        ArrayList<JavaPairRDD<String, Tuple2<Integer,String>>> ClassifiedAlicesRDDs = new ArrayList<>();
        for (int i = 1; i<=3; i++)
            ClassifiedAlicesRDDs.add(BlockingAlgorithm.mapClassify(AliceRDDs.get(i-1),ReferenceSets.get(i-1) , String.valueOf(i)));

        ArrayList<JavaPairRDD<String, Tuple2<Integer,String>>> ClassifiedBobsRDDs = new ArrayList<>();

        for (int i = 1; i<=3; i++)
            ClassifiedBobsRDDs.add(BlockingAlgorithm.mapClassify(BobRDDs.get(i-1),ReferenceSets.get(i-1) , String.valueOf(i)));

        //data in rdds is like (recordID , Tuple(score , class) )

        JavaPairRDD<String, Iterable<Tuple2<Integer, String>>> BobsRDDGrouped =  ClassifiedBobsRDDs.get(0).union(ClassifiedBobsRDDs.get(1).union(ClassifiedBobsRDDs.get(2))).groupByKey() ;
        JavaPairRDD<String, Iterable<Tuple2<Integer, String>>> AlicesRDDGrouped =  ClassifiedAlicesRDDs.get(0).union(ClassifiedAlicesRDDs.get(1).union(ClassifiedAlicesRDDs.get(2))).groupByKey() ;

        JavaPairRDD<String, Tuple2<String, Integer>>BobsblocksRDD = BobsRDDGrouped.flatMapToPair(BlockingAlgorithm::combineBlocks);
        JavaPairRDD<String, Tuple2<String, Integer>> AliceblocksRDD = AlicesRDDGrouped.flatMapToPair(BlockingAlgorithm::combineBlocks);

        // combine the 2 different databases Alices and Bob.
        JavaPairRDD<String, Tuple2<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>> CombinedBlocks= BobsblocksRDD.cogroup(AliceblocksRDD) ;

        //filter block which have only one source
        JavaPairRDD<String, Tuple2<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>> filteredBlocks = CombinedBlocks.filter(block -> {
            return block._2()._1().iterator().hasNext() && block._2()._2().iterator().hasNext() ;
        });

        //map blocks to <String , List<String> >
        JavaPairRDD<String,Iterable<Tuple2<String, Integer>>> finalBlocks = filteredBlocks.mapValues(block -> Stream.concat(StreamSupport.stream(block._1().spliterator(),true),
                StreamSupport.stream(block._2().spliterator(),true)).sorted(Comparator.comparingInt(Tuple2::_2)).collect(Collectors.toList()));



        JavaPairRDD<String,Integer> predictedMatchesRDD = finalBlocks.flatMapToPair(block -> {

                List<Tuple2<String,Integer>> records = new ArrayList<>() ;
                Iterator<Tuple2<String, Integer>>  it = block._2().iterator() ;

                Tuple2<String, Integer> currentBA = it.next();
                String record ;
                while (true) {
                    Tuple2<String, Integer> nextBA;

                    if (it.hasNext()) {
                        nextBA = it.next() ;
                        record = currentBA._1().concat(nextBA._1()) ;
                        records.add(new Tuple2<>(record,1)) ;
                        currentBA = nextBA ;
                    } else
                        break;


                 }

                return records.iterator();
        });


        JavaPairRDD<String, Integer> matchesCount = predictedMatchesRDD.reduceByKey(Integer::sum) ;

        matchesCount.collect().forEach(System.out::println);



        Scanner myscanner = new Scanner(System.in);
        myscanner.nextLine();

        sc.close();

    }
}