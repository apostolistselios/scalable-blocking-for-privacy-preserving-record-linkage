package com.algorithms;

import com.model.Block;
import com.model.BlockElement;
import com.model.BlockingAttribute;
import com.utils.BinarySearch;
import com.utils.Conf;
import com.utils.DurstenfeldShuffle;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;

public abstract class ReferenceSetBlocking {

	public static Dataset<Block> blocking(Dataset<Row> AliceDS, Dataset<Row> BobDS, Dataset<Row> ReferenceSets, SparkSession spark){

		// classify for each dataset
		Dataset<BlockingAttribute> classifiedAliceDS = spark.emptyDataset(Encoders.bean(BlockingAttribute.class)) ;
		Dataset<BlockingAttribute> classifiedBobDS = spark.emptyDataset(Encoders.bean(BlockingAttribute.class)) ;

		int s = 1 ; // count for samples
		for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++) {
			List<String> referenceSetsList = ReferenceSets.select(col("col" + i))
					.na().drop().distinct().filter((FilterFunction<Row>) row -> row.getString(0).length() > 1 )
					.map((MapFunction<Row, String>)  row -> row.getString(0).toUpperCase(), Encoders.STRING())
					.collectAsList();


			for(int j = 0; j < Conf.NUM_OF_SAMPLES; j++) {

				List<String> reference_set_values =  DurstenfeldShuffle.shuffle(referenceSetsList).stream().limit(Conf.RS_SIZE).sorted().collect(Collectors.toList());

				if (i == 1 && j == 0){
					classifiedAliceDS = classifyBlockingAttribute(
							AliceDS.select(col(Conf.ID), col(getColumnName(i))),
							reference_set_values,
							String.valueOf(s),
							"A") ;

					classifiedBobDS = classifyBlockingAttribute(
							BobDS.select(col(Conf.ID), col(getColumnName(i))),
							reference_set_values,
							String.valueOf(s),
							"B") ;
				}else{
					classifiedAliceDS = classifiedAliceDS.union(classifyBlockingAttribute(
							AliceDS.select(col(Conf.ID), col(getColumnName(i))),
							reference_set_values,
							String.valueOf(s),
							"A")) ;
					classifiedBobDS = classifiedBobDS.union(classifyBlockingAttribute(
							BobDS.select(col(Conf.ID), col(getColumnName(i))),
							reference_set_values,
							String.valueOf(s),
							"B"));
				}
				s ++ ;
			}
		}
        /*
         * data in classifiedBobDS  is like
         * +-------+---------+-----+
           |classID| recordID|score|
           +-------+---------+-----+
           |   S1.1| BAT24345|    6|
           |   S2.1| BAT24345|    7|
           |   S1.1|BAA169148|    8|
         */

		Dataset<Row> groupedAliceDS = classifiedAliceDS.groupBy(col("recordID")).agg(collect_list("classID"),
				collect_list("score"));

		Dataset<Row> groupedBobDS = classifiedBobDS.groupBy(col("recordID")).agg(collect_list("classID"),
				collect_list("score"));

        /*
         * data in BobsRDD for grouped and classified records  is like
         * +---------+---------------------+--------------------+
           | recordID|collect_list(classID)| collect_list(score)|
           +---------+---------------------+--------------------+
           |BAA151425| [S1.1, S4.1, S7.2...|[5, 6, 5, 4, 6, 7...|
           |BAA178497| [S3.2, S6.1, S9.1...|[5, 4, 5, 4, 7, 3...|
           |BAA122891| [S1.1, S4.2, S7.2...|[6, 7, 5, 6, 6, 5...|
         */

		Dataset<Row> bobBlocksDS = groupedBobDS.flatMap(ReferenceSetBlocking::createBlockIDs,
				com.utils.Encoders.preBlockElement())
				.groupBy(col("blockID"))
				.agg(collect_list("recordID").as("recordIDListB"),
						collect_list("score").as("scoreListB"))
				.withColumnRenamed("blockID", "blockIDb");

		Dataset<Row> aliceBlocksDS = groupedAliceDS.flatMap(ReferenceSetBlocking::createBlockIDs,
				com.utils.Encoders.preBlockElement())
				.groupBy(col("blockID"))
				.agg(collect_list("recordID").as("recordIDListA"),
						collect_list("score").as("scoreListA"))
				.withColumnRenamed("blockID", "blockIDa");
        /*
         * data in BobsblocksDS  is like
         *  +---------+----------------------+-------------------+
            |  blockIDb|      recordIDListB  |         scoreListB|
            +---------+----------------------+-------------------+
            |S6.1-S9.2|           [BAA151425]|               [10]|
            |S3.1-S8.3|  [BAA122891, BAA10...|       [12, 12, 13]|
            |S2.1-S5.3|           [BAA103168]|               [11]|
         */

		// combine the 2 different databases Alices and Bob.
		Dataset<Row> combinedBlocks = bobBlocksDS.join(aliceBlocksDS,
				bobBlocksDS.col("blockIDb").equalTo(aliceBlocksDS.col("blockIDa")),
				"inner").drop("blockIDa");

        /*
        +---------+--------------------+----------+--------------------+----------+
        | blockIDb|       recordIDListB|scoreListB|       recordIDListA|scoreListA|
        +---------+--------------------+----------+--------------------+----------+
        |S3.2-S8.2|         [BAA151425]|       [7]|[AAA165159, AAA17...|   [11, 9]|
        |S4.2-S7.1|         [BAA178497]|      [14]|         [AAA178497]|      [13]|
        |S6.1-S9.2|         [BAA151425]|      [10]|         [AAA151425]|      [11]|
         */
		Dataset<Block> blocks = combinedBlocks.map((MapFunction<Row, Block>) ReferenceSetBlocking::sortBlockElements, Encoders.bean(Block.class)) ;

		return blocks ;
	}

	private static String getColumnName(int i) {
		switch(i){
			case 1:
				return "surname" ;
			case 2:
				return "name" ;
			case 3:
				return "city" ;
			default:
				System.out.println("1111 Wrong i");
				return  null ;
		}
	}

	public static Dataset<BlockingAttribute> classifyBlockingAttribute(Dataset<Row> baDS,
	                                                                   List<String> rs,
	                                                                   String rsnum,
	                                                                   String prefix) {

		return baDS.map((MapFunction<Row,BlockingAttribute>) ba -> {
			String classID;
			int pos;
			int num_of_search_chars;

			if (ba.getString(1).length() > 1)
				num_of_search_chars = Conf.NUM_OF_BINARY_SEARCH_CHARS + 1  ;
			else
				num_of_search_chars = Conf.NUM_OF_BINARY_SEARCH_CHARS ;

			pos = BinarySearch.binarySearch(rs,0,rs.size()-1, ba.getString(1), num_of_search_chars)  ;

			int d1 = 1000000;
			if (pos -1 > 0 ) {
				d1 = LevenshteinDistance.getDefaultInstance().apply(ba.getString(1), rs.get(pos-1)) ;
			}

			int d2 = LevenshteinDistance.getDefaultInstance().apply(ba.getString(1), rs.get(pos)) ;
			// return
			if (d1 < d2 ) {
				classID = "S" + rsnum + "." + pos; //(pos-1) +1
				return new BlockingAttribute(classID, prefix + ba.getString(0), d1);
			}
			else {
				classID = "S" + rsnum + "." + (pos + 1); // (pos) + 1
				return new BlockingAttribute(classID,prefix + ba.getString(0) , d2);
			}
		},Encoders.bean(BlockingAttribute.class));
	}


	public static Block sortBlockElements(Row block){

		ArrayList<BlockElement> baList = new ArrayList<>();

		List<Object> bListID = block.getList(1);
		List<Object> bListScore = block.getList(2);
		List<Object> aListID = block.getList(3);
		List<Object> aListScore = block.getList(4);

		for(int i=0; i < bListID.size(); i++){
			baList.add(new BlockElement(String.valueOf(bListID.get(i)), Integer.parseInt(String.valueOf(bListScore.get(i))))) ;
		}

		for(int i=0; i < aListID.size(); i++){
			baList.add(new BlockElement(String.valueOf(aListID.get(i)), Integer.parseInt(String.valueOf(aListScore.get(i))))) ;
		}
		Block blockObj = new Block(block.getString(0), baList);
		blockObj.calculateRank();
		Collections.sort(blockObj.getBAList());
		return blockObj;
	}

	public static Iterator<Row> createBlockIDs(Row row) {

		List<Row> blocks = new ArrayList<>();

		//Class Ids
		List<String> classIDList = row.getList(1);

		// scores
		List<Integer> scoreList = row.getList(2);

		for (int i = 0; i < classIDList.size(); i++) {
			String blockID;
			String currClassID = classIDList.get(i);
			String nextClassID = classIDList.get((i + 1) % classIDList.size());

			// a simple rule to keep consistency in naming of block combinations :
			// first part in BlockID is always the smaller class lexicographically
			if (currClassID.compareTo(nextClassID) > 0)
				blockID = nextClassID + "-" + currClassID;
			else
				blockID = currClassID + "-" + nextClassID;

			blocks.add(RowFactory.create(blockID ,
					row.getString(0),
					scoreList.get(i) + scoreList.get((i + 1) % scoreList.size()))) ;
		}
		return blocks.iterator();
	}
}
