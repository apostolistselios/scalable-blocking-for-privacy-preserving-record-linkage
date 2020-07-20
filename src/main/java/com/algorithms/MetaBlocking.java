package com.algorithms;


import com.model.Block;
import com.model.BlockElement;
import com.utils.BloomAlgorithms;
import com.utils.Conf;
import com.utils.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

public abstract class MetaBlocking  {

	public MetaBlocking () {}

	public static Dataset<Row> metaBocking(SparkSession spark,
	                                       Dataset<Row> alicesDS,
	                                       Dataset<Row> bobsDS,
	                                       Dataset<Block> blocks) {

		// Create datasets with records' blooms filters
		Dataset<Row> AliceBloomsDS = alicesDS.map((MapFunction<Row, Row>) MetaBlocking::createBloomFilters, Encoders.bloomFilter()) ;
		Dataset<Row> BobBloomsDS = bobsDS.map((MapFunction<Row, Row>) MetaBlocking::createBloomFilters, Encoders.bloomFilter()) ;

		// create possibleMatchesDS that contains only unique rows
		Dataset<Row> possibleMatchesDS = blocks.flatMap(MetaBlocking::createPossibleMatches, Encoders.possibleMatches()).distinct();

		Dataset<Row> possibleMatchesWithBloomsDS = possibleMatchesDS.join(AliceBloomsDS,
				possibleMatchesDS.col("record1").equalTo(AliceBloomsDS.col("recordID")), "inner")
				.drop("recordID").withColumnRenamed("bloom","bloom1")
				.join(BobBloomsDS,possibleMatchesDS.col("record2").equalTo(BobBloomsDS.col("recordID")), "inner")
				.drop("recordID").withColumnRenamed("bloom","bloom2") ;

		Dataset<Row> matches = possibleMatchesWithBloomsDS.filter((FilterFunction<Row>) MetaBlocking::isMatch).drop("bloom1", "bloom2");

		return matches;
	}

	public static Iterator<Row> createPossibleMatches(Block block){
		List<Row> recordPairs = new ArrayList<>();
		List<BlockElement> baList = block.getBAList();

		for (int i = 1; i < baList.size(); i++) {
			String record1 = baList.get(i).getRecordID();
			int windowLimit = Conf.WINDOW_SIZE;
			for (int j = i - 1; j >= i - windowLimit + 1 && j >= 0; j--) {
				String record2 = baList.get(j).getRecordID();

				char firstcharOfrecord1 = record1.charAt(0);
				char firstcharOfrecord2 = record2.charAt(0);

				// check if records are from  different database
				if (firstcharOfrecord1 != firstcharOfrecord2) {

					// put records in the right column and remove prefix
					if (firstcharOfrecord1 == 'A')
						recordPairs.add(RowFactory.create(record1.substring(1),record2.substring(1)));
					else
						recordPairs.add(RowFactory.create(record2.substring(1),record1.substring(1)));
				}
				else {
					windowLimit++;
				}
			}
		}
		return recordPairs.iterator();
	}


	public static Row createBloomFilters(Row record) {
//		// join all attribute
//		String recordString = " ";

		byte[][] bloomFilters = new byte[Conf.NUM_OF_BLOCKING_ATTRS][] ;
		for(int i= 0 ; i < Conf.NUM_OF_BLOCKING_ATTRS ; i ++ )
			bloomFilters[i] =  BloomAlgorithms.string2Bloom(record.getString(i+1)).getFilter().toByteArray() ;


		return RowFactory.create(record.get(0), bloomFilters );
//		return RowFactory.create(record.get(0), bf.getFilter().toByteArray() );
	}


	public static boolean isMatch(Row row) {

		List<byte[]> bf1 = row.getList(2) ;
		List<byte[]> bf2 = row.getList(3) ;

		int matchedFields=0;
		for(int i =0 ; i < Conf.NUM_OF_BLOCKING_ATTRS; i++){
			if (matchField(BitSet.valueOf(bf1.get(i)), BitSet.valueOf(bf2.get(i)), Conf.MATCHING_THRESHOLD))
				matchedFields++;
		}

		return matchedFields >= Conf.MATCHES_TO_ACCEPT;

	}

	private static boolean matchField(BitSet filterA, BitSet filterB, float t){
		float diceCo = 0f;
		int bf1Card = filterA.cardinality();
		int bf2Card = filterB.cardinality();

		filterA.and(filterB);
		int commons = filterA.cardinality();

		diceCo =  2 *(float) commons / (bf1Card + bf2Card) ;

		return diceCo >= t;
	}


}
