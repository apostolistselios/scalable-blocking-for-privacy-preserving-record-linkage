package main;


import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Simulator {

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<List<String>> input = new ArrayList<>();

        List<String> s1 = Arrays.asList("anthony", "lawrence", "victor", "zoe");
        List<String> s2 = Arrays.asList("alex", "dorothy", "jonathan", "naomi");
        List<String> s3 = Arrays.asList("alex", "john", "rhonda", "tristan");

        Collections.sort(s1);
        Collections.sort(s2);
        Collections.sort(s3);

        // Alice's data
        input.add(Arrays.asList("a1", "nicholas", "smith", "madrid"));
        input.add(Arrays.asList("a2", "ann", "cobb", "london"));

        // Bob's data
        input.add(Arrays.asList("b1", "kevin", "anderson", "warsaw"));
        input.add(Arrays.asList("b2", "anne", "cobb", "london"));

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<List<String>> inputRDD = sc.parallelize(input);

        /*
         * JavaPairRDD<String, String> pairsRDD = inputRDD.flatMapToPair( x -> {
         * List<Tuple2<String, String>> pairs = new ArrayList() ;
         *
         * for(String ba : x.subList(1, x.size())) { pairs.add(new Tuple2<String,
         * String>(x.get(0),ba)) ; } return pairs.iterator();
         *
         * }) ;
         */

        JavaPairRDD<String, String> name_pairsRDD = mapBlockingAttributes(inputRDD, 1);

        JavaPairRDD<String, String> last_name_pairsRDD = mapBlockingAttributes(inputRDD, 2);

        JavaPairRDD<String, String> city_pairsRDD = mapBlockingAttributes(inputRDD, 3);

//		JavaPairRDD<String, String> pairsRDD = name_pairsRDD.union(last_name_pairsRDD.union(city_pairsRDD)) ;

        JavaPairRDD<String, List<String>> name_classifiedRDD = mapClassify(name_pairsRDD, s1, "1");

        JavaPairRDD<String, List<String>> last_name_classifiedRDD = mapClassify(last_name_pairsRDD, s2, "2");

        JavaPairRDD<String, List<String>> city_classifiedRDD = mapClassify(city_pairsRDD, s3, "3");

        JavaPairRDD<String, List<String>> classifiedRDD = name_classifiedRDD.union(last_name_classifiedRDD.union(city_classifiedRDD)) ;

        List<Tuple2<String, Iterable<List<String>>>> results = classifiedRDD.groupByKey().collect(); // must implement reduceByKey() not groupByKey


        results.forEach(System.out::println);

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

        return inputRDD.mapToPair(listOfBAs -> new Tuple2<>(listOfBAs.get(0), listOfBAs.get(whichBAtomap)));

    }


    public static JavaPairRDD<String, List<String>> mapClassify(JavaPairRDD<String, String> baRDD, List<String> rs,
                                                                String rsnum) {
        return baRDD.mapValues(ba -> {

            String c;
            int pos;


            pos = binarySearch(rs,0,rs.size()-1, ba)  ;// should implement binary search on prefixes here to work


            int d1 = 1000000 ;
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

    public static int binarySearch(List<String> arr, int l, int r, String x)
    {

        if (r >= l) {
            int mid = l +(int) Math.ceil((double) (r -1) / (double) 2 ) ;

            // If the element is present at the
            // middle itself
            if (arr.get(mid).substring(0,3).equals(x.substring(0,3)) )
                return mid;


            // If element is smaller than mid, then
            // it can only be present in right subarray
            if ( x.substring(0,3).compareTo(arr.get(mid).substring(0,3)) > 0  ) {
                return binarySearch(arr, mid + 1, r, x);
            }
            else if (mid == arr.size()-1) {
                if ( LevenshteinDistance.getDefaultInstance().apply(x.substring(0,3), arr.get(mid).substring(0,3)) >
                        LevenshteinDistance.getDefaultInstance().apply(x, arr.get(mid-1).substring(0,3)))
                    return mid ;
                else
                    return mid -1 ;
            }


            // Else the element can only be present
            // in left subarray


            return binarySearch(arr, l, mid-1, x);
        }

        // We reach here when element is not present
        // in array
        if (r < 0) {return r +1  ;}
        else if (r >3) {return r-1 ; }
        else  return r  ;

    }




}