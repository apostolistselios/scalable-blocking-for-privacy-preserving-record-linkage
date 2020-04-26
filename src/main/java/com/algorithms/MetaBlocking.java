package com.algorithms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.utils.Block;
import com.utils.BlockingAttribute;

import scala.Tuple2;

public class MetaBlocking implements Serializable {
	
	private static final long serialVersionUID = 5317646661733959435L;

	public MetaBlocking () {}
	
	public JavaPairRDD<String, Integer> predict(JavaRDD<Block> blocks) {
		return blocks.flatMapToPair(new PairFlatMapFunction<Block, String, Integer> (){
			private static final long serialVersionUID = 1L;

			public Iterator<Tuple2<String, Integer>> call(Block block) {
				ArrayList<BlockingAttribute> baList = block.getBAList();
				ArrayList<Tuple2<String, Integer>> matches = new ArrayList<>();
				
				for (BlockingAttribute ba : baList) {
					if (baList.indexOf(ba) + 1 != baList.size()) {
						String match = ba.getRecordID() + "-" + baList.get(baList.indexOf(ba) + 1).getRecordID();
						matches.add(new Tuple2<String, Integer>(match, 1));
					} else { 
						break;
					}
				}
				
				return matches.iterator();
			}
		});
	}
}
