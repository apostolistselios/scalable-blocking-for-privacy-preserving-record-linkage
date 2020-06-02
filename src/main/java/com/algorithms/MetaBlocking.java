package com.algorithms;

import com.utils.Bigrams;
import com.utils.Block;
import com.utils.BlockElement;
import com.utils.Conf;
import info.debatty.java.stringsimilarity.SorensenDice;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.util.sketch.BloomFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MetaBlocking implements Serializable {
	
	private static final long serialVersionUID = 5317646661733959435L;

	public MetaBlocking () {}
	

	public Iterator<Row> createPossibleMatches(Block block, int window){
		List<Row> recordPairs = new ArrayList<>();
		List<BlockElement> baList = block.getBAList();

		for (int i = 1; i < baList.size(); i++) {
			String record1 = baList.get(i).getRecordID();
			for (int j = i - 1; j >= i - window + 1 && j >= 0; j--) {
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
			}
		}
		return recordPairs.iterator();
	}


	public Row createBloomFilters(List<String> record) {
		// join all attribute
		List<String> attributesBigrams = Bigrams.ngrams(2, String.join("", record.subList(1, Conf.NUMBER_OF_BLOCKING_ATTRS + 1)));
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


	public boolean isMatch(Row row,double THRESHOLD) throws  Exception{
		byte[] bf1 = row.getAs("bloom1");
		byte[] bf2 = row.getAs("bloom2");

		SorensenDice sd = new SorensenDice();
		double dCof = sd.similarity(Arrays.toString(bf1), Arrays.toString(bf2));

		// (double) (2 * BitSet.valueOf(Bytes.concat(bf1, bf2)).cardinality()) / (BitSet.valueOf(bf1).cardinality() + BitSet.valueOf(bf2).cardinality());
		return dCof > THRESHOLD;
	}
}
