package com.algorithms;

import com.utils.Bigrams;
import com.utils.Block;
import com.utils.BlockElement;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.util.sketch.BloomFilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
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
			for (int j = i - 1; j >= i - window && j >= 0; j--) {
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
		List<String> attributesBigrams = Bigrams.ngrams(2, String.join("", record.subList(1, 4)));
		// create bloom filter
		BloomFilter bf = BloomFilter.create(attributesBigrams.size(), 900);

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

		return RowFactory.create(record.get(0), BitSet.valueOf(bloomByte.toByteArray()).toString());
	}
}
