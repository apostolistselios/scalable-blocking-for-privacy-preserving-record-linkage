package com.algorithms;

import com.utils.Block;
import com.utils.BlockElement;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

public class MetaBlocking implements Serializable {
	
	private static final long serialVersionUID = 5317646661733959435L;

	public MetaBlocking () {}
	
	public JavaPairRDD<String, Integer> predict(JavaRDD<Block> blocks , int window , Dataset<Row> Alice , Dataset<Row> Bob) {
		return blocks.flatMapToPair(new PairFlatMapFunction<Block, String, Integer> (){
			private static final long serialVersionUID = 1L;

			public Iterator<Tuple2<String, Integer>> call(Block block) {
				ArrayList<BlockElement> baList = block.getBAList();
				ArrayList<Tuple2<String, Integer>> matches = new ArrayList<>();


				for(int i = 1 ; i < baList.size() ; i ++ ) {
					String record1 = baList.get(i).getRecordID();
					for(int j = i - 1 ; j >= i - window && j >=0 ; j--) {
						String record2 = baList.get(j).getRecordID();
						char firstcharOfrecord1 = record1.charAt(0) ;
						char firstcharOfrecord2 = record2.charAt(0) ;

						if(firstcharOfrecord1 != firstcharOfrecord2){
							System.out.println(record1.substring(1) + " " + record2.substring(1));
							Row record1Attributes ;
							Row record2Attributes ;
							if(firstcharOfrecord1 == 'A') {
								record1Attributes = Alice.filter(Alice.col("id").equalTo(record1.substring(1))).first();
								record2Attributes = Bob.filter(Alice.col("id").equalTo(record2.substring(1))).first();
							}
							else{
								record1Attributes = Bob.filter(Alice.col("id").equalTo(record1.substring(1))).first();
								record2Attributes = Alice.filter(Alice.col("id").equalTo(record2.substring(1))).first();
							}

							if (record1Attributes.getString(0).equals(record2Attributes.getString(0)))
								matches.add(new Tuple2<String, Integer>(record1Attributes.getString(0), 1)) ;

							System.out.println(record1Attributes.getString(0));

						}



					}
				}
//				for (BlockingAttribute ba : baList) {
//					if (baList.indexOf(ba) + 1 != baList.size()) {
//						String match = ba.getRecordID() + "-" + baList.get(baList.indexOf(ba) + 1).getRecordID();
//						matches.add(new Tuple2<String, Integer>(match, 1));
//					} else {
//						break;
//					}
//				}

				return matches.iterator();
			}
		});
	}
}
