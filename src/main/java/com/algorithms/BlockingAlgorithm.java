package com.algorithms;

import com.utils.BinarySearch;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.utils.BinarySearch.*;

public class BlockingAlgorithm {
    public static JavaPairRDD<String, String> mapBlockingAttributes(JavaRDD<List<String>> inputRDD, int whichBAtomap) {
        // method returns pairs of [a1, nicholas], [a2, ann], etc.
        return inputRDD.mapToPair(listOfBAs -> new Tuple2<>(listOfBAs.get(0), listOfBAs.get(whichBAtomap)));
    }


    public static JavaPairRDD<String, List<String>> mapClassify(JavaPairRDD<String, String> baRDD, List<String> rs,
                                                                String rsnum) {
        return baRDD.mapValues(ba -> {

            String c;
            int pos;


            pos = BinarySearch.binarySearch(rs,0,rs.size()-1, ba)  ;// should implement binary search on prefixes here to work


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
    public static Iterator<Tuple2<String, String>> combineBlocks(Tuple2<String, Iterable<List<String>>> TuppleOfbas) {
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
