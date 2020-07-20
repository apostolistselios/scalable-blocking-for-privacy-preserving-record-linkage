package com.utils;

public abstract class Conf {



    public static String HDFS_DIRECTORY = "hdfs://master:9000/user/user/blocking/db/";
    public static String DB_A_FILE_NAME = "main_A_25p_50k.csv";
    public static String DB_B_FILE_NAME = "main_B_25p_50k.csv";
//    public static String RS_FILE_NAME = "Master.csv";
    public static String RS_FILE_NAME = "baseball18000doubled.csv";

    public static int NUM_OF_BLOCKING_ATTRS = 3;

    // Use attributes that match NUM_OF_BLOCKING_ATTRS.
    // For example if NUM_OF_BLOCKING_ATTRS = 3 use ATTR_1, ATTR_2, ATTR_3.
    // Always remember to change method SQLData.query!
    public static String ID = "id";
    public static String ATTR_1 = "surname"; // surname
    public static String ATTR_2 = "name"; // name
    public static String ATTR_3 = "city"; // city
    public static String ATTR_4 = "precinct";
    public static String ATTR_5 = "middle_name";
    public static String ATTR_6 = "age";

    public static int DB_SIZE = 50000;
    public static double SAMPLE_FRACTION = 1.0 ;
    public static int RS_SIZE = (int) (DB_SIZE * 0.001);
    public static int NUM_OF_BINARY_SEARCH_CHARS = 1;
    public static int WINDOW_SIZE = 20;
    public static float MATCHING_THRESHOLD = 0.45f;
    public static double COMMON_RECORDS = 0.25;
    public static int BLOOM_FILTER_SIZE = 900;
    public static int NUM_OF_SAMPLES = 10;
    public static int HASH_FUNCTIONS = 4 ;
    public static int nGramSize = 2 ;
    public static int bloomFilterSize = 150 ;
    public static int M_N_RATIO = 20 ;
    public static int MATCHES_TO_ACCEPT = 3;

}
