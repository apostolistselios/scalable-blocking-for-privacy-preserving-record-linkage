package com.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public abstract class Encoders {

    public static ExpressionEncoder<Row> bloomFilter() {
        // define the schema for blooms dataset
        StructType bloomsFilterSchema = new StructType();
        bloomsFilterSchema = bloomsFilterSchema.add("recordID", DataTypes.StringType, false);
        bloomsFilterSchema = bloomsFilterSchema.add("bloom", DataTypes.createArrayType(DataTypes.BinaryType) , false);
        return RowEncoder.apply(bloomsFilterSchema);
    }

    public static ExpressionEncoder<Row> possibleMatches() {
        // define the schema for possible matches dataset
        StructType possiblesMatchesSchema = new StructType();
        // this is the column for records from Alice's database
        possiblesMatchesSchema = possiblesMatchesSchema.add("record1", DataTypes.StringType, false);
        // this is the column for records from Bob;s database
        possiblesMatchesSchema = possiblesMatchesSchema.add("record2", DataTypes.StringType, false);
        return RowEncoder.apply(possiblesMatchesSchema);
    }

    public static ExpressionEncoder<Row> preBlockElement() {
        StructType schema = new StructType();
        schema = schema.add("blockID", DataTypes.StringType, false);
        schema = schema.add("recordID", DataTypes.StringType, false);
        schema = schema.add("score", DataTypes.IntegerType, false);
        return RowEncoder.apply(schema);
    }

}
