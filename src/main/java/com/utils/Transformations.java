package com.utils;

import com.model.BlockingAttribute;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class Transformations {

    public static JavaRDD<List<String>> datasetToRDD(Dataset<Row> alicesDS, String prefix) {
        return alicesDS.toJavaRDD().map(row -> {
            List<String> list = new ArrayList<>();
            // change id to include source
            list.add(prefix + row.getString(0));
            for (int i = 1; i < row.size(); i++) {
                list.add(row.getString(i));
            }
            return list;
        });
    }

    public static JavaPairRDD<String, Iterable<BlockingAttribute>> groupRDDs(ArrayList<JavaPairRDD<String, BlockingAttribute>> rdd){
        JavaPairRDD<String, BlockingAttribute> temp = rdd.get(0);

        for (int i = 1; i < rdd.size(); i++){
            temp = temp.union(rdd.get(i));
        }

        return temp.groupByKey();
    }

    public static Dataset<BlockingAttribute> groupDSs(ArrayList<Dataset<BlockingAttribute>> ds) {
        Dataset<BlockingAttribute> temp =  ds.get(0);

        for (int i = 1; i < Conf.NUM_OF_BLOCKING_ATTRS; i++){
            temp = temp.union(ds.get(i));
        }

        return temp;
    }

}
