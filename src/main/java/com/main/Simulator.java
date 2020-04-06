package com.main;

import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import scala.Tuple2;

public class Simulator {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		List<List<String>> input = new ArrayList<List<String>>();
		
		List<String> s1 = Arrays.asList("anthony", "lawrence", "victor", "zoe") ; 
		List<String> s2 = Arrays.asList("alex", "dorothy", "jonathan", "naomi") ; 
		List<String> s3 = Arrays.asList("alex", "john", "rhonda", "tristan") ; 
		// Alice's data
		input.add(Arrays.asList("a1","nicholas", "smith", "madrid"));
		input.add(Arrays.asList("a2" ,"ann", "cobb", "london"));
		
		// Bob's data
		input.add(Arrays.asList("b1", "kevin", "anderson", "warsaw"));
		input.add(Arrays.asList("b2", "anne", "cobb", "london"));

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf); 
		
		JavaRDD<List<String>> inputRDD = sc.parallelize(input) ; 
		
//		JavaPairRDD<String, String> pairsRDD = inputRDD.flatMapToPair( x -> {
//			List<Tuple2<String, String>> pairs = new ArrayList() ; 
//			
//			for(String ba : x.subList(1, x.size())) {
//				pairs.add(new Tuple2<String, String>(x.get(0),ba)) ; 
//			}
//			return pairs.iterator();
//					
//		}) ; 
		
		JavaPairRDD<String, String>  name_pairsRDD = mapBlockingAtrributes(inputRDD, 1) ;		
		
		JavaPairRDD<String, String>  last_name_pairsRDD = mapBlockingAtrributes(inputRDD, 2) ; 
		
		JavaPairRDD<String, String>  city_pairsRDD = mapBlockingAtrributes(inputRDD, 3);
		
		JavaPairRDD<String, String> pairsRDD = name_pairsRDD.union(last_name_pairsRDD.union(city_pairsRDD)) ;
		
		
		List<Tuple2<String, String>> results = pairsRDD.collect(); 
		
		
		results.forEach(System.out::println);
		
		JavaPairRDD<String,String> resultsRDD = sc.parallelizePairs(results) ; 
		
		Long count = resultsRDD.count() ; 
		
		System.out.println(count);
		
		Scanner myscanner = new Scanner(System.in); 
		myscanner.nextLine(); 
	
		sc.close();
		
	}
	
	
	 public static JavaPairRDD<String, String> mapBlockingAtrributes(JavaRDD<List<String>> inputRDD,int whichBAtomap){
		 
		 
		return inputRDD.mapToPair(listOfBAs -> {
			return new Tuple2<String,String> (listOfBAs.get(0), listOfBAs.get(whichBAtomap)) ; 
		});
		 
	 }

}
