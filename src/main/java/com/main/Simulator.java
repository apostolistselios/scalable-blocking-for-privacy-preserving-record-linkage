package com.main;


import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.algorithms.BlockingAlgorithm.*;

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

        JavaRDD<List<String>> AlicesRDD = sc.parallelize(Alice_DB);
        JavaRDD<List<String>> BobsRDD = sc.parallelize(Bob_DB);
        /*
         * JavaPairRDD<String, String> pairsRDD = inputRDD.flatMapToPair( x -> {
         * List<Tuple2<String, String>> pairs = new ArrayList() ;
         *
         * for(String ba : x.subList(1, x.size())) { pairs.add(new Tuple2<String,
         * String>(x.get(0),ba)) ; } return pairs.iterator();
         *
         * }) ;
         */
        // Add all Alice's RDDs created in a list name_pairsRDD, last_nameRDD, etc.

        ArrayList<JavaPairRDD<String, String>> AliceRDDs = new ArrayList<JavaPairRDD<String, String>>();
        for (int i = 1; i<=3; i++)
            AliceRDDs.add(mapBlockingAttributes(AlicesRDD, i));

        ArrayList<JavaPairRDD<String, String>> BobRDDs = new ArrayList<JavaPairRDD<String, String>>();
        for (int i = 1; i<=3; i++)
            BobRDDs.add(mapBlockingAttributes(BobsRDD, i));


        // classify for each
        // get the name_pairsRDD,last_nameRDD, etc. and classify it respectively with 1st reference set, 2nd, etc and add it into an ArrayList.
        ArrayList<JavaPairRDD<String, List<String>>> ClassifiedAlicesRDDs = new ArrayList<JavaPairRDD<String, List<String>>>();
        for (int i = 1; i<=3; i++)
            ClassifiedAlicesRDDs.add(mapClassify(AliceRDDs.get(i-1),ReferenceSets.get(i-1) , String.valueOf(i)));

        ArrayList<JavaPairRDD<String, List<String>>> ClassifiedBobsRDDs = new ArrayList<JavaPairRDD<String, List<String>>>();

        for (int i = 1; i<=3; i++)
            ClassifiedBobsRDDs.add(mapClassify(BobRDDs.get(i-1),ReferenceSets.get(i-1) , String.valueOf(i)));

        JavaPairRDD<String, Iterable<List<String>>> BobsRDDGrouped =  ClassifiedBobsRDDs.get(0).union(ClassifiedBobsRDDs.get(1).union(ClassifiedBobsRDDs.get(2))).groupByKey() ;
        JavaPairRDD<String, Iterable<List<String>>> AlicesRDDGrouped =  ClassifiedAlicesRDDs.get(0).union(ClassifiedAlicesRDDs.get(1).union(ClassifiedAlicesRDDs.get(2))).groupByKey() ;
        // combine the 2 different databases Alices and Bob.
        JavaPairRDD<String, Iterable<List<String>>> ClassifiedCombinedRDD = BobsRDDGrouped.union(AlicesRDDGrouped);

        JavaPairRDD<String, String> blocksRDD = ClassifiedCombinedRDD.flatMapToPair(Simulator::combineBlocks);

        JavaPairRDD<String, Iterable<String>> results = blocksRDD.groupByKey().filter(block -> {
            boolean sourceA = false ;
            boolean sourceB = false ;

            for(String recordID : block._2()){
                if(recordID.contains("a")){
                    sourceA = true ;
                }else if(recordID.contains("b"))
                    sourceB = true ;
            }

            return sourceA && sourceB ;
        });


        results.collect().forEach(System.out::println);

        /*
         * JavaPairRDD<String,String> resultsRDD = sc.parallelizePairs(results) ;
         *
         * Long count = resultsRDD.count() ;
         *
         * System.out.println(count);
         */

        Scanner myscanner = new Scanner(System.in);
        myscanner.nextLine();

        sc.close();

    }

    public static JavaPairRDD<String, String> mapBlockingAttributes(JavaRDD<List<String>> inputRDD, int whichBAtomap) {
        // method returns pairs of [a1, nicholas], [a2, ann], etc.
        return inputRDD.mapToPair(listOfBAs -> new Tuple2<>(listOfBAs.get(0), listOfBAs.get(whichBAtomap)));
    }


    public static JavaPairRDD<String, List<String>> mapClassify(JavaPairRDD<String, String> baRDD, List<String> rs,
                                                                String rsnum) {
        return baRDD.mapValues(ba -> {

            String c;
            int pos;


            pos = binarySearch(rs,0,rs.size()-1, ba)  ;// should implement binary search on prefixes here to work


            int d1 = 10 ^ 6  ;
            if (pos -1 > 0 ) {
                d1 = LevenshteinDistance.getDefaultInstance().apply(ba, rs.get(pos-1)) ;
            }

            int d2 = LevenshteinDistance.getDefaultInstance().apply(ba,rs.get(pos)) ;

            if (d1 <= d2 ) {
                c = "S" + rsnum + "." + pos; //(pos-1) +1
            }
            else c ="S" + rsnum + "." + (pos + 1); // (pos) + 1

            return Arrays.asList(ba, c);
        });

    }

    public static int binarySearch(List<String> arr, int low, int high, String x)
    {

        if (high >= low) {
            int mid = low +(int) Math.ceil((double) (high - low ) / (double) 2 ) ;

            // If the element is present at the
            // middle itself
            if (arr.get(mid).substring(0,3).equals(x.substring(0,3)) )
                return mid;


            // If element is smaller than mid, then
            // it can only be present in right subarray
            if ( x.substring(0,3).compareTo(arr.get(mid).substring(0,3)) > 0  ) {
                return binarySearch(arr, mid + 1, high, x);
            }
            else if (mid == arr.size()-1) {
                return mid;
            }


            // Else the element can only be present
            // in left subarray


            return binarySearch(arr, low, mid-1, x);
        }

        // We reach here when element is not present
        // in array
        if (high < 0) {return high +1  ;}
        else if (high >3) {return high-1 ; }
        else  return high  ;

    }


    private static Iterator<Tuple2<String, String>> combineBlocks(Tuple2<String, Iterable<List<String>>> TuppleOfbas) {
        ArrayList<Tuple2<String, String>> blocks = new ArrayList<>();

        Iterator<List<String>> it = TuppleOfbas._2().iterator();
        List<String> currentBA = it.next();
        List<String> firstBA = currentBA; // keep first to combine it with the last
        while (true) {
            List<String> nextBA;
            String block;
            if (it.hasNext()) {
                nextBA = it.next();
                block = currentBA.get(1) + "-" + nextBA.get(1);
                blocks.add(new Tuple2<>(block, TuppleOfbas._1()));
                currentBA = nextBA;
            } else {
                block = currentBA.get(1) + "-" + firstBA.get(1);
                blocks.add(new Tuple2<>(block, TuppleOfbas._1()));
                break;
            }
        }
        return blocks.iterator();
    }
}