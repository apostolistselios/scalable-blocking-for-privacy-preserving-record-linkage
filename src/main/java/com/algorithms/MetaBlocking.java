package com.algorithms;

import com.model.Block;
import com.model.BlockElement;
import com.utils.Bigrams;
import com.utils.Conf;
import com.utils.Encoders;
import info.debatty.java.stringsimilarity.SorensenDice;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.sketch.BloomFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public abstract class MetaBlocking implements Serializable {
	
	private static final long serialVersionUID = 5317646661733959435L;

	public MetaBlocking () {}

	public static Dataset<Row> metaBocking(SparkSession spark,
										   JavaRDD<List<String>> alicesRDD,
										   JavaRDD<List<String>> bobsRDD,
										   JavaRDD<Block> blocks) {

		// Create datasets with records' blooms filters
		Dataset<Row> AliceBloomsDS = spark.createDataset(
				alicesRDD.map(MetaBlocking::createBloomFilters).rdd()
				, Encoders.bloomFilter()
		);
		Dataset<Row> BobsBloomsDS = spark.createDataset(
				bobsRDD.map(MetaBlocking::createBloomFilters).rdd()
				, Encoders.bloomFilter()
		);

		// create possibleMatchesDS that contains only unique rows
		Dataset<Row> possibleMatchesDS = spark.createDataset(
				blocks.flatMap(MetaBlocking::createPossibleMatches).rdd()
				, Encoders.possibleMatches()
		).distinct();

		Dataset<Row> possibleMatchesWithBloomsDS = possibleMatchesDS.join(AliceBloomsDS,
				possibleMatchesDS.col("record1").equalTo(AliceBloomsDS.col("recordID")))
				.drop("recordID").withColumnRenamed("bloom","bloom1")
				.join(BobsBloomsDS,possibleMatchesDS.col("record2").equalTo(BobsBloomsDS.col("recordID")))
				.drop("recordID").withColumnRenamed("bloom","bloom2") ;

		// Transformation takes the majority of execution time (~300secs)
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

					// put records in the right column
					if (firstcharOfrecord1 == 'A')
						recordPairs.add(RowFactory.create(record1,record2));
					else
						recordPairs.add(RowFactory.create(record2,record1));
				}
				else {
					windowLimit++;
				}
			}
		}
		return recordPairs.iterator();
	}


	public static Row createBloomFilters(List<String> record) {
		// join all attribute
		List<String> attributesBigrams = Bigrams.ngrams(2, String.join("", record.subList(1, Conf.NUM_OF_BLOCKING_ATTRS + 1)));
		// create bloom filter
		BloomFilter bf = BloomFilter.create(attributesBigrams.size(), Conf.BLOOM_FILTER_SIZE);

		// put bigrams in bloom filters
		for (String bigram : attributesBigrams)
			bf.putString(bigram);

		// reformat bloom filter as string
		ByteArrayOutputStream bloomByte = new ByteArrayOutputStream();
		try {
			bf.writeTo(bloomByte);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return RowFactory.create(record.get(0), bloomByte.toByteArray() );
	}


	public static boolean isMatch(Row row) throws  Exception{
		byte[] bf1 = row.getAs("bloom1");
		byte[] bf2 = row.getAs("bloom2");

		SorensenDice sd = new SorensenDice();
		double dCof = sd.similarity(Arrays.toString(bf1), Arrays.toString(bf2));

		// (double) (2 * BitSet.valueOf(Bytes.concat(bf1, bf2)).cardinality()) / (BitSet.valueOf(bf1).cardinality() + BitSet.valueOf(bf2).cardinality());
		return dCof > Conf.MATCHING_THRESHOLD;
	}
}
