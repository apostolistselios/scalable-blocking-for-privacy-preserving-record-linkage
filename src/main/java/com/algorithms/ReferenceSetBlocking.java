package com.algorithms;

import com.model.Block;
import com.model.BlockElement;
import com.model.BlockingAttribute;
import com.utils.BinarySearch;
import com.utils.Conf;
import com.utils.DurstenfeldShuffle;
import com.utils.Transformations;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;
import scala.collection.Map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.spark.sql.functions.col;

public abstract class ReferenceSetBlocking implements Serializable {
	
	private static final long serialVersionUID = -998419074815274020L;

    public static JavaRDD<Block> blocking(JavaRDD<List<String>> AlicesRDD, JavaRDD<List<String>> BobsRDD, Dataset<Row> ReferenceSets){
        ArrayList<JavaPairRDD<String, String>> AliceRDDs = mapBlockingAttributes(AlicesRDD);
        ArrayList<JavaPairRDD<String, String>> BobRDDs = mapBlockingAttributes(BobsRDD);

        /*
         * data in BobsRDD for attribute name is like
         * (AT24345,ABEDE)
         * (AA181290,ACO0TA)
         */

        // classify for each
        // get the name_pairsRDD, last_nameRDD, etc. and classify it respectively with 1st reference set, 2nd, etc and add it into an ArrayList.
        ArrayList<JavaPairRDD<String, BlockingAttribute>> ClassifiedAlicesRDDs = new ArrayList<>(), ClassifiedBobsRDDs = new ArrayList<>();
        classify(AliceRDDs, BobRDDs, ClassifiedAlicesRDDs, ClassifiedBobsRDDs, ReferenceSets);

        /*
         * data in BobsRDD for classified attribute name is like
         * (AT24345,BA(S1.2,null,13))
         * (AA181290,BA(S1.2,null,13))
         */

        // data in rdds is like (recordID , BlockingAttribute(classID, score))
        JavaPairRDD<String, Iterable<BlockingAttribute>> BobsRDDGrouped = Transformations.groupRDDs(ClassifiedBobsRDDs);
        JavaPairRDD<String, Iterable<BlockingAttribute>> AlicesRDDGrouped = Transformations.groupRDDs(ClassifiedAlicesRDDs);

        /*
         * data in BobsRDD for grouped and classified records  is like
         * (AA181290,[BA(S1.2,null,13), BA(S2.1,null,14), BA(S3.1,null,15)])
         * (AT24345,[BA(S1.2,null,13), BA(S2.1,null,14), BA(S3.1,null,15)])
         */

        JavaPairRDD<String, BlockElement> BobsblocksRDD = BobsRDDGrouped.flatMapToPair(ReferenceSetBlocking::combineBlocks);
        JavaPairRDD<String, BlockElement> AliceblocksRDD = AlicesRDDGrouped.flatMapToPair(ReferenceSetBlocking::combineBlocks);

        /*
         * data in BobsblocksRDD  is like
         * (S1.2-S2.1,BA(S1.2,AA181290,13))
         */

        // combine the 2 different databases Alices and Bob.
        JavaPairRDD<String, Tuple2<Iterable<BlockElement>, Iterable<BlockElement>>> CombinedBlocks = BobsblocksRDD.cogroup(AliceblocksRDD);

        // filter block which have only one source
        JavaPairRDD<String, Tuple2<Iterable<BlockElement>, Iterable<BlockElement>>> filteredBlocks = CombinedBlocks.filter(block -> {
            return block._2()._1().iterator().hasNext() && block._2()._2().iterator().hasNext();
        });

        /*
         * Data in filteredBlocks is like
         * (S3.1-S1.2,([BA(S3.1,AA181290,15), BA(S3.1,AT24345,15)],[BA(S3.1,AA181290,15), BA(S3.1,AT24345,15)]))
         */

        // Data in CombinedBlocks are like  last BobsblocksRDD representation but includes records from both dbs
        JavaRDD<Block> blocks = filteredBlocks.map(ReferenceSetBlocking::sortBlockElements);

        /*
         * Data in blocks is like
         * [BLOCK: S3.1-S1.2 - Rank: 60 - [BA(S3.1,BAT24345,15), BA(S3.1,BAA181290,15), BA(S3.1,AAA181290,15), BA(S3.1,AAT24345,15)]]
         */
        return blocks;
    }

//    public Tuple2<String,String> mapBlockingAttributes(List<String> record , int ba ){
//        return new Tuple2<>(record.get(0), record.get(ba));
//    }

    public static ArrayList<JavaPairRDD<String, String>> mapBlockingAttributes(JavaRDD<List<String>> rdd) {
        ArrayList<JavaPairRDD<String, String>> temp = new ArrayList<>();
        for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++) {
            // copy i to final variable just to pass it as parameter
            int finalI = i;
            temp.add(rdd.mapToPair(record -> new Tuple2<>(record.get(0), record.get(finalI))));
        }

        return temp;
    }

//    public  Dataset<Row> mapBlockingAttributes(Dataset<Row> dbDS , int ba ){
//	    if (ba==1)
//            return dbDS.select(Conf.getProperty("ID"), Conf.getProperty("ATTR_2"));
//	    else if (ba ==2)
//            return dbDS.select(Conf.getProperty("ID"), Conf.getProperty("ATTR_1"));
//	    else
//            return dbDS.select(Conf.getProperty("ID"), Conf.getProperty("ATTR_3"));
//    }

    public static Dataset<Row> mapBlockingAttributes(Dataset<Row> dbDS , int ba ) throws NoSuchFieldException, IllegalAccessException {
        String attributeName = (String) Conf.class.getField("ATTR_" + ba).get(null);

        return dbDS.select(Conf.ID, attributeName);
    }

    public static void classify(ArrayList<JavaPairRDD<String, String>> rddA,
                         ArrayList<JavaPairRDD<String, String>> rddB,
                         ArrayList<JavaPairRDD<String, BlockingAttribute>> classifiedRddA,
                         ArrayList<JavaPairRDD<String, BlockingAttribute>> classifiedRddB,
                         Dataset<Row> referenceSets) {

        int s = 1 ; // count for samples
        for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++) {
            List<String> referenceSetsList = referenceSets.select(col("col" + i))
                    .na().drop().distinct().filter((FilterFunction<Row>) row -> row.getString(0).length() > 0 )
                    .map((MapFunction<Row, String>)  row -> row.getString(0).toUpperCase(), Encoders.STRING())
                    .collectAsList();

            for(int j = 0; j < Conf.NUM_OF_SAMPLES; j++) {

                List<String> reference_set_values =  DurstenfeldShuffle.shuffle(referenceSetsList).stream().limit(Conf.RS_SIZE).sorted().collect(Collectors.toList());


                classifiedRddA.add(classifyBlockingAttribute(
                        rddA.get(i - 1)
                        , reference_set_values
                        , String.valueOf(s)));
                classifiedRddB.add(classifyBlockingAttribute(
                        rddB.get(i - 1)
                        , reference_set_values
                        , String.valueOf(s)));

                s ++ ;
            }


        }
    }

    public static JavaPairRDD<String, BlockingAttribute> classifyBlockingAttribute(JavaPairRDD<String, String> baRDD, List<String> rs,
                                                                            String rsnum) {
        return baRDD.mapValues(ba -> {
            String classID;
            int pos;

            pos = BinarySearch.binarySearch(rs,0,rs.size()-1, ba)  ;// should implement binary search on prefixes here to work

            int d1 = 1000000;
            if (pos -1 > 0 ) {
                d1 = LevenshteinDistance.getDefaultInstance().apply(ba, rs.get(pos-1)) ;
            }

            int d2 = LevenshteinDistance.getDefaultInstance().apply(ba, rs.get(pos)) ;
            // return a Record object
            if (d1 < d2 ) {
                classID = "S" + rsnum + "." + pos; //(pos-1) +1
                return new BlockingAttribute(classID, d1);
            }
            else {
                classID = "S" + rsnum + "." + (pos + 1); // (pos) + 1
                return new BlockingAttribute(classID, d2);
            }
        });
    }

    public static Dataset<BlockingAttribute> classify(Dataset<Row> baDS , List<String> rs,
                                               String rsnum){
        return baDS.map((MapFunction<Row, BlockingAttribute>) row -> {
            String ba = row.getString(1) ;

            String classID;
            int pos;

            pos = BinarySearch.binarySearch(rs,0,rs.size()-1, ba)  ;// should implement binary search on prefixes here to work

            int d1 = 1000000;
            if (pos -1 > 0 ) d1 = LevenshteinDistance.getDefaultInstance().apply(ba, rs.get(pos - 1));

            int d2 = LevenshteinDistance.getDefaultInstance().apply(ba, rs.get(pos)) ;
            // return a Record object
            if (d1 <= d2 ) {
                classID = "S" + rsnum + "." + pos; //(pos-1) +1
                return new BlockingAttribute(classID,row.getString(0), d1);
            }
            else {
                classID = "S" + rsnum + "." + (pos + 1); // (pos) + 1
                return new BlockingAttribute(classID,row.getString(0),d2);
            }
        }, Encoders.bean(BlockingAttribute.class));
    }
    
    public static Iterator<Tuple2<String, BlockElement>> combineBlocks(Tuple2<String, Iterable<BlockingAttribute>> baTuple) {
        ArrayList<Tuple2<String, BlockElement>> blocks = new ArrayList<>();

        Iterator<BlockingAttribute> it = baTuple._2().iterator();
        BlockingAttribute currentBA = it.next();
        BlockingAttribute firstBA = currentBA; // keep first to combine it with the last
        while (true) {
            BlockingAttribute nextBA;
            String blockID;
            if (it.hasNext()) {
                nextBA = it.next();
                if (currentBA.getClassID().compareTo(nextBA.getClassID()) > 0)
                    blockID = nextBA.getClassID() + "-" + currentBA.getClassID();
                else
                    blockID = currentBA.getClassID() + "-" + nextBA.getClassID();

                currentBA.setRecordID(baTuple._1());
                blocks.add(new Tuple2<>(blockID, new BlockElement(currentBA.getRecordID(),currentBA.getScore() + nextBA.getScore())));
                currentBA = nextBA;
            } else {
                if (currentBA.getClassID().compareTo(firstBA.getClassID()) > 0)
                    blockID = firstBA.getClassID() + "-" + currentBA.getClassID();
                else
                    blockID = currentBA.getClassID() + "-" + firstBA.getClassID();
                currentBA.setRecordID(baTuple._1());
                blocks.add(new Tuple2<>(blockID, new BlockElement(currentBA.getRecordID(),currentBA.getScore() + firstBA.getScore())));
                break;
            }
        }
        return blocks.iterator();
    }

    public static Iterator<Tuple2<String, BlockElement>> combineBlocks2(Tuple2<String, Iterable<BlockingAttribute>> baTuple) {
        ArrayList<Tuple2<String, BlockElement>> blocks = new ArrayList<>();

        List<BlockingAttribute> blockingAttributesList = new ArrayList<>();
        baTuple._2().forEach(blockingAttributesList::add);
        int score = 0 ;
        for(BlockingAttribute ba : blockingAttributesList){ score += ba.getScore(); }


        int i = 0 ;
        while (true){
            String blockID;
            if (i == blockingAttributesList.size() -1){

                blockID = blockingAttributesList.get(i).getClassID() + "-" + blockingAttributesList.get(0).getClassID();
                blocks.add(new Tuple2<>(blockID, new BlockElement(baTuple._1(),score)));
                break;
            }
            blockID = blockingAttributesList.get(i).getClassID() + "-" + blockingAttributesList.get(i+1).getClassID();
            blocks.add(new Tuple2<>(blockID, new BlockElement(baTuple._1(),score)));
            i ++ ;
        }

        return blocks.iterator();
    }

    public static Block sortBlockElements(Tuple2<String, Tuple2<Iterable<BlockElement>, Iterable<BlockElement>>> block){
        ArrayList<BlockElement> baList = (ArrayList<BlockElement>) Stream.concat(StreamSupport.stream(block._2()._1().spliterator(), true),
                StreamSupport.stream(block._2()._2().spliterator(), true)).collect(Collectors.toList());

        Block blockObj = new Block(block._1(), baList);
        blockObj.calculateRank();
        Collections.sort(blockObj.getBAList());
        return blockObj;
    }

    public static Iterator<Row> combineBlocksDS(Row row) {

        List<Row> blocks = new ArrayList<>();

        //Class Ids
        List<String> classIDlist = row.getList(1);

        // scores
        List<Integer> scorelist = row.getList(2);

        for (int i = 0; i < Conf.NUM_OF_BLOCKING_ATTRS; i++) {
            String blockID;
            String currClassID = classIDlist.get(i);
            String nextClassID = classIDlist.get((i + 1) % Conf.NUM_OF_BLOCKING_ATTRS);

            // a simple rule to keep consistency in naming of block combinations :
            // first part in BlockID is always the smaller class lexicographically
            if (currClassID.compareTo(nextClassID) > 0)
                blockID = nextClassID + "-" + currClassID;
            else
                blockID = currClassID + "-" + nextClassID;

            // map witch represent a record  <recordID, Summed score>
            Map<String, Integer> record = new scala.collection.immutable.Map.Map1<>(
                    row.getString(0),
                    scorelist.get(i) + scorelist.get((i + 1) % Conf.NUM_OF_BLOCKING_ATTRS));


            blocks.add(RowFactory.create(blockID, record));
        }
        return blocks.iterator();
    }
}
