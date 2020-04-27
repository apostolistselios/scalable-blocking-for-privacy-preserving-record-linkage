package com.algorithms;

import com.utils.BinarySearch;
import com.utils.BlockingAttribute;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ReferenceSetBlocking implements Serializable {
	
	private static final long serialVersionUID = -998419074815274020L;

	public ReferenceSetBlocking() {}
	
    public JavaPairRDD<String, String> mapBlockingAttributes(JavaRDD<List<String>> inputRDD, int ba) {
        // method returns pairs of [Record ID, BA Value] e.g. [a1, nicholas], [a2, ann], etc.
        return inputRDD.mapToPair(record -> new Tuple2<>(record.get(0), record.get(ba)));
    }

    public  Dataset<Row> mapBlockingAttributes(Dataset<Row> dbDS , int ba ){
	    if (ba==1)
            return dbDS.select("id", "name");
	    else if (ba ==2)
            return dbDS.select("id", "surname");
	    else
            return dbDS.select("id", "city");
    }

    public JavaPairRDD<String, BlockingAttribute> classify(JavaPairRDD<String, String> baRDD, List<String> rs,
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
            if (d1 <= d2 ) {
                classID = "S" + rsnum + "." + pos; //(pos-1) +1
                return new BlockingAttribute(classID, d1);
            }
            else {
                classID = "S" + rsnum + "." + (pos + 1); // (pos) + 1
                return new BlockingAttribute(classID, d2);
            }
        });
    }

    public Dataset<BlockingAttribute> classify(Dataset<Row> baDS , List<String> rs,
                                               String rsnum){
        return baDS.map((MapFunction<Row, BlockingAttribute>) s -> {
            String ba = s.getString(1) ;

            String classID;
            int pos;

            pos = BinarySearch.binarySearch(rs,0,rs.size()-1, ba)  ;// should implement binary search on prefixes here to work

            int d1 = 1000000;
            if (pos -1 > 0 ) d1 = LevenshteinDistance.getDefaultInstance().apply(ba, rs.get(pos - 1));

            int d2 = LevenshteinDistance.getDefaultInstance().apply(ba, rs.get(pos)) ;
            // return a Record object
            if (d1 <= d2 ) {
                classID = "S" + rsnum + "." + pos; //(pos-1) +1
                return new BlockingAttribute(classID,s.getString(0), d1);
            }
            else {
                classID = "S" + rsnum + "." + (pos + 1); // (pos) + 1
                return new BlockingAttribute(classID,s.getString(0),d2);
            }
        }, Encoders.bean(BlockingAttribute.class));
    }
    
    public Iterator<Tuple2<String, BlockingAttribute>> combineBlocks(Tuple2<String, Iterable<BlockingAttribute>> baTuple) {
        ArrayList<Tuple2<String,BlockingAttribute>> blocks = new ArrayList<>();

        Iterator<BlockingAttribute> it = baTuple._2().iterator();
        BlockingAttribute currentBA = it.next();
        BlockingAttribute firstBA = currentBA; // keep first to combine it with the last
        while (true) {
            BlockingAttribute nextBA;
            String blockID;
            if (it.hasNext()) {
                nextBA = it.next();
                blockID = currentBA.getClassID() + "-" + nextBA.getClassID();
                currentBA.setRecordID(baTuple._1());
                blocks.add(new Tuple2<>(blockID, currentBA));
                currentBA = nextBA;
            } else {
                blockID = currentBA.getClassID() + "-" + firstBA.getClassID();
                currentBA.setRecordID(baTuple._1());
                blocks.add(new Tuple2<>(blockID, currentBA));
                break;
            }
        }
        return blocks.iterator();
    }
}