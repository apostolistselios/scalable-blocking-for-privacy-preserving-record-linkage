package com.algorithms;

import com.utils.BinarySearch;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.*;

public class BlockingAlgorithm {
    public static JavaPairRDD<String, String> mapBlockingAttributes(JavaRDD<List<String>> inputRDD, int whichBAtomap) {
        // method returns pairs of [a1, nicholas], [a2, ann], etc.
        return inputRDD.mapToPair(listOfBAs -> new Tuple2<>(listOfBAs.get(0), listOfBAs.get(whichBAtomap)));
    }


    public static JavaPairRDD<String, Tuple2<Integer, String>> mapClassify(JavaPairRDD<String, String> baRDD, List<String> rs,
                                                                           String rsnum) {
        return baRDD.mapValues(ba -> {

            String c;
            int pos;


            pos = BinarySearch.binarySearch(rs,0,rs.size()-1, ba)  ;// should implement binary search on prefixes here to work


            int d1 = 0xf4240;
            if (pos -1 > 0 ) {
                d1 = LevenshteinDistance.getDefaultInstance().apply(ba, rs.get(pos-1)) ;
            }

            int d2 = LevenshteinDistance.getDefaultInstance().apply(ba,rs.get(pos)) ;

            //TODO an object called record

            // return a tuple , score as key and a list [ba,class] (e.g. (4, [a2,S1])
            if (d1 <= d2 ) {
                c = "S" + rsnum + "." + pos; //(pos-1) +1
                return new Tuple2<>(d1, c);
            }
            else {
                c = "S" + rsnum + "." + (pos + 1); // (pos) + 1
                return new Tuple2<>(d2, c);
            }

        });
    }
    public static Iterator<Tuple2<String, Tuple2<String, Integer>>> combineBlocks(Tuple2<String, Iterable<Tuple2<Integer, String>>> TuppleOfbas) {
        ArrayList<Tuple2<String, Tuple2<String,Integer>>> blocks = new ArrayList<>();

        Iterator<Tuple2<Integer, String>> it = TuppleOfbas._2().iterator();
        Tuple2<Integer, String> currentBA = it.next();
        Tuple2<Integer, String> firstBA = currentBA; // keep first to combine it with the last
        while (true) {
            Tuple2<Integer, String> nextBA;
            String block;
            if (it.hasNext()) {
                nextBA = it.next();
                block = currentBA._2() + "-" + nextBA._2();
                // add a tuple , as a key the combined Block id and as values a tuple ,  the record id as key and the score (e.g. (S3.2-S1.1, (b2,4)) )
                blocks.add(new Tuple2<>(block,new Tuple2<>(TuppleOfbas._1(),currentBA._1())));
                currentBA = nextBA;
            } else {
                block = currentBA._2() + "-" + firstBA._2();
                blocks.add(new Tuple2<>(block,new Tuple2<>(TuppleOfbas._1(),currentBA._1())));
                break;
            }
        }
        // we return a list of blocks like (S3.2-S1.1, (b2,4))
        return blocks.iterator();
    }
}
