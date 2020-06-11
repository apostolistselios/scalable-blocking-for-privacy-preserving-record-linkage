package com.algorithms;

import com.model.Block;
import com.model.BlockElement;
import com.model.BlockingAttribute;
import com.utils.*;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Encoders;
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
        classifyRDDs(AliceRDDs, BobRDDs, ClassifiedAlicesRDDs, ClassifiedBobsRDDs, ReferenceSets);

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

        JavaPairRDD<String, BlockElement> BobsblocksRDD = BobsRDDGrouped.flatMapToPair(ReferenceSetBlocking::combineRDDBlocks);
        JavaPairRDD<String, BlockElement> AliceblocksRDD = AlicesRDDGrouped.flatMapToPair(ReferenceSetBlocking::combineRDDBlocks);

        /*
         * data in BobsblocksRDD  is like
         * (S1.2-S2.1,BA(S1.2,AA181290,13))
         */

        // combine the 2 different databases Alices and Bob.
        JavaPairRDD<String, Tuple2<Iterable<BlockElement>, Iterable<BlockElement>>> CombinedBlocks = BobsblocksRDD.cogroup(AliceblocksRDD);

        // filter block which have only one source
        JavaPairRDD<String, Tuple2<Iterable<BlockElement>, Iterable<BlockElement>>> filteredBlocks = CombinedBlocks.filter(block ->
                block._2()._1().iterator().hasNext() && block._2()._2().iterator().hasNext()
        );

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

    public static void blocking (Dataset<Row> AlicesDS, Dataset<Row> BobsDS,
                                 Dataset<Row> ReferenceSets)
            throws NoSuchFieldException, IllegalAccessException {

        // distribute blocking attributes in different datasets
        ArrayList<Dataset<Row>> AliceDSs = ReferenceSetBlocking.mapBlockingAttributes(AlicesDS);
        ArrayList<Dataset<Row>> BobDSs = ReferenceSetBlocking.mapBlockingAttributes(BobsDS);
        /*
         * data in ds is like
         * example of Bob's db for attribute name
         * +---+-----+
         * | id| name|
         * +---+-----+
         * | b1|kevin|
         * | b2| anne|
         * +---+-----+
         */

        // classify for each
        // classify respectively for every blocking attribute with 1st reference set, 2nd, etc and add it into an ArrayList.
        ArrayList<Dataset<BlockingAttribute>> ClassifiedAlicesDSs = new ArrayList<>();
        ArrayList<Dataset<BlockingAttribute>> ClassifiedBobsDSs = new ArrayList<>();

        ReferenceSetBlocking.classifyDatasets(AliceDSs, BobDSs, ClassifiedAlicesDSs, ClassifiedBobsDSs, ReferenceSets);
        /*
         * data in ds is like
         * example of classification for name as blocking attribute for Bob's db
         *  +-------+--------+-----+
         *  |classID|recordID|score|
         *  +-------+--------+-----+
         *  |   S1.1|      b1|    6|
         *  |   S1.1|      b2|    4|
         *  +-------+--------+-----+
         */

        Dataset<BlockingAttribute> AlicesDSGroupped = Transformations.groupDSs(ClassifiedAlicesDSs);
        Dataset<BlockingAttribute> BobsDSGroupped = Transformations.groupDSs(ClassifiedBobsDSs);

        BobsDS = BobsDSGroupped
                .groupBy("recordID").agg(functions.collect_list("classID"), functions.collect_list("score"))
                .flatMap(ReferenceSetBlocking::combineDSBlocks, com.utils.Encoders.blocks());

        AlicesDS = AlicesDSGroupped
                .groupBy("recordID").agg(functions.collect_list("classID"), functions.collect_list("score"))
                .flatMap(ReferenceSetBlocking::combineDSBlocks, com.utils.Encoders.blocks());
        /*
         * After groupBy data in ds is like : example of grouping for Bob's db
         *       +--------+---------------------+-------------------+
         *       |recordID|collect_list(classID)|collect_list(score)|
         *       +--------+---------------------+-------------------+
         *       |      b2|   [S3.2, S1.1, S2.1]|          [4, 4, 4]|
         *       |      b1|   [S3.4, S2.1, S1.1]|          [5, 6, 6]|
         *       +--------+---------------------+-------------------+
         *
         * After flatmap data in ds is like : example for Bob's Blocks
         *       +---------+----------+
         *       |  blockID|    record|
         *       +---------+----------+
         *       |S1.1-S2.1| [b2 -> 8]|
         *       |S1.1-S3.2| [b2 -> 8]|
         *       |S2.1-S3.2| [b2 -> 8]|
         *       |S1.1-S2.1|[b1 -> 12]|
         *       |S1.1-S3.4|[b1 -> 11]|
         *       |S2.1-S3.4|[b1 -> 11]|
         *       +---------+----------+
         */

        //TODO: GroupBy blockID , sort on records of every block and return a single Dataset
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

    public static ArrayList<Dataset<Row>> mapBlockingAttributes(Dataset<Row> dbDS) throws NoSuchFieldException, IllegalAccessException {
        ArrayList<Dataset<Row>> temp = new ArrayList<>();

        for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++) {
            String attributeName = (String) Conf.class.getField("ATTR_" + i).get(null);
            temp.add(dbDS.select(Conf.ID, attributeName));
        }

        return temp;
    }

    public static void classifyRDDs(ArrayList<JavaPairRDD<String, String>> rddA,
                                    ArrayList<JavaPairRDD<String, String>> rddB,
                                    ArrayList<JavaPairRDD<String, BlockingAttribute>> classifiedRddA,
                                    ArrayList<JavaPairRDD<String, BlockingAttribute>> classifiedRddB,
                                    Dataset<Row> referenceSets) {

        int s = 1 ; // count for samples
        for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++) {
            List<String> referenceSetsList = referenceSets.select(col("col" + i)).distinct().as(Encoders.STRING()).collectAsList();
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

    public static void classifyDatasets(ArrayList<Dataset<Row>> DSA, ArrayList<Dataset<Row>> DSB,
                                ArrayList<Dataset<BlockingAttribute>> classifiedDSA,
                                ArrayList<Dataset<BlockingAttribute>> classifiedDSB, Dataset<Row> referenceSets) {

        for (int i = 1; i <= Conf.NUM_OF_BLOCKING_ATTRS; i++) {

            classifiedDSA.add(ReferenceSetBlocking.classifyBlockingAttribute(
                    DSA.get(i - 1)
                    , referenceSets.select(col("col" + i)).as(Encoders.STRING()).collectAsList()
                    , String.valueOf(i)));

            classifiedDSB.add(ReferenceSetBlocking.classifyBlockingAttribute(
                    DSB.get(i - 1)
                    , referenceSets.select(col("col" + i)).as(Encoders.STRING()).collectAsList()
                    , String.valueOf(i)));
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
                d1 = LevenshteinDistance.getDefaultInstance().apply(ba.toLowerCase(), rs.get(pos-1).toLowerCase()) ;
            }

            int d2 = LevenshteinDistance.getDefaultInstance().apply(ba.toLowerCase(), rs.get(pos).toLowerCase()) ;
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

    public static Dataset<BlockingAttribute> classifyBlockingAttribute(Dataset<Row> baDS , List<String> rs,
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
    
    public static Iterator<Tuple2<String, BlockElement>> combineRDDBlocks(Tuple2<String, Iterable<BlockingAttribute>> baTuple) {
        ArrayList<Tuple2<String, BlockElement>> blocks = new ArrayList<>();

        Iterator<BlockingAttribute> it = baTuple._2().iterator();
        BlockingAttribute currentBA = it.next();
        BlockingAttribute firstBA = currentBA; // keep first to combine it with the last
        while (true) {
            BlockingAttribute nextBA;
            if (it.hasNext()) {
                nextBA = it.next();
                mergeBlock(baTuple, blocks, currentBA, nextBA);
                currentBA = nextBA;
            } else {
                mergeBlock(baTuple, blocks, currentBA, firstBA);
                break;
            }
        }
        return blocks.iterator();
    }

    private static void mergeBlock(Tuple2<String, Iterable<BlockingAttribute>> baTuple, ArrayList<Tuple2<String, BlockElement>> blocks, BlockingAttribute currentBA, BlockingAttribute firstBA) {
        String blockID;
        if (currentBA.getClassID().compareTo(firstBA.getClassID()) > 0)
            blockID = firstBA.getClassID() + "-" + currentBA.getClassID();
        else
            blockID = currentBA.getClassID() + "-" + firstBA.getClassID();
        currentBA.setRecordID(baTuple._1());
        blocks.add(new Tuple2<>(blockID, new BlockElement(currentBA.getRecordID(),currentBA.getScore() + firstBA.getScore())));
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

    public static Iterator<Row> combineDSBlocks(Row row) {

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
