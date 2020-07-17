package com.algorithms;

import com.model.Block;
import com.model.BlockElement;
import com.model.BlockingAttribute;
import com.utils.BinarySearch;
import com.utils.Conf;
import com.utils.DurstenfeldShuffle;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;

public abstract class ReferenceSetBlocking implements Serializable {
	
	private static final long serialVersionUID = -998419074815274020L;

    public static JavaRDD<Block> blocking(Dataset<Row> AliceDS, Dataset<Row> BobDS, Dataset<Row> ReferenceSets, SparkSession spark){

        // classify for each dataset
        Dataset<BlockingAttribute> classifiedAliceDS = spark.emptyDataset(Encoders.bean(BlockingAttribute.class)) ;
        Dataset<BlockingAttribute> classifiedBobDS = spark.emptyDataset(Encoders.bean(BlockingAttribute.class)) ;

        int s = 1 ; // count for samples
        for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++) {
            List<String> referenceSetsList = ReferenceSets.select(col("col" + i))
                    .na().drop().distinct().filter((FilterFunction<Row>) row -> row.getString(0).length() > 0 )
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

        Dataset<Row> BobBlocksDS = groupedBobDS.flatMap(ReferenceSetBlocking::createBlockIDs, com.utils.Encoders.preBlockElement());
        Dataset<Row> AliceBlocksDS = groupedAliceDS.flatMap(ReferenceSetBlocking::createBlockIDs, com.utils.Encoders.preBlockElement());
        
        System.exit(0);

        /*
         * data in BobsblocksDS  is like
         * +---------+---------+-----+
           |  blockID| recordID|score|
           +---------+---------+-----+
           |S1.2-S4.1|BAA151425|   14|
           |S4.1-S7.2|BAA151425|   12|
           |S3.1-S7.2|BAA151425|   12|
         */
//
//        // combine the 2 different databases Alices and Bob.
//        JavaPairRDD<String, Tuple2<Iterable<BlockElement>, Iterable<BlockElement>>> CombinedBlocks = BobsblocksRDD.cogroup(AliceblocksRDD);
//
//        // filter block which have only one source
//        JavaPairRDD<String, Tuple2<Iterable<BlockElement>, Iterable<BlockElement>>> filteredBlocks = CombinedBlocks.filter(block -> {
//            return block._2()._1().iterator().hasNext() && block._2()._2().iterator().hasNext();
//        });
//
//        /*
//         * Data in filteredBlocks is like
//         * (S3.1-S1.2,([BA(S3.1,AA181290,15), BA(S3.1,AT24345,15)],[BA(S3.1,AA181290,15), BA(S3.1,AT24345,15)]))
//         */
//
//        // Data in CombinedBlocks are like  last BobsblocksRDD representation but includes records from both dbs
//        JavaRDD<Block> blocks = filteredBlocks.map(ReferenceSetBlocking::sortBlockElements);
//
//        /*
//         * Data in blocks is like
//         * [BLOCK: S3.1-S1.2 - Rank: 60 - [BA(S3.1,BAT24345,15), BA(S3.1,BAA181290,15), BA(S3.1,AAA181290,15), BA(S3.1,AAT24345,15)]]
//         */
//        return blocks;
        return null ;
    }


    public static ArrayList<JavaPairRDD<String, String>> mapBlockingAttributes(JavaRDD<List<String>> rdd) {
        ArrayList<JavaPairRDD<String, String>> temp = new ArrayList<>();
        for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++) {
            // copy i to final variable just to pass it as parameter
            int finalI = i;
            temp.add(rdd.mapToPair(record -> new Tuple2<>(record.get(0), record.get(finalI))));
        }

        return temp;
    }

    public static Dataset<Row> mapBlockingAttributes(Dataset<Row> dbDS , int ba ) throws NoSuchFieldException, IllegalAccessException {
        String attributeName = (String) Conf.class.getField("ATTR_" + ba).get(null);

        return dbDS.select(Conf.ID, attributeName);
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


            pos = BinarySearch.binarySearch(rs,0,rs.size()-1, ba.getString(1))  ;

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


    public static Block sortBlockElements(Tuple2<String, Tuple2<Iterable<BlockElement>, Iterable<BlockElement>>> block){
        ArrayList<BlockElement> baList = (ArrayList<BlockElement>) Stream.concat(StreamSupport.stream(block._2()._1().spliterator(), true),
                StreamSupport.stream(block._2()._2().spliterator(), true)).collect(Collectors.toList());

        Block blockObj = new Block(block._1(), baList);
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

            blocks.add(RowFactory.create(blockID, row.getString(0), scoreList.get(i) + scoreList.get((i + 1) % scoreList.size())));
        }
        return blocks.iterator();
    }
}
