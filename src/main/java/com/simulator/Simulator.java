package com.simulator;


import com.algorithms.MetaBlocking;
import com.algorithms.ReferenceSetBlocking;
import com.database.SQLData;
import com.model.Block;
import com.utils.Conf;
import com.utils.Timer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

public class Simulator {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("JavaRDD")
				.set("spark.executor.memory", "5g")
				.set("spark.debug.maxToStringFields", String.valueOf(100));

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		SQLData db = new SQLData(spark);


		Dataset<Row> AliceDS = db.getAlice();
		Dataset<Row> BobDS = db.getBob();

		Dataset<Row> ReferenceSets = db.getReferenceSet();

		AliceDS.persist(StorageLevel.DISK_ONLY());
		BobDS.persist(StorageLevel.DISK_ONLY());

		ReferenceSets.cache();
		Timer.start();

		Dataset<Block> blocks = ReferenceSetBlocking.blocking(AliceDS, BobDS, ReferenceSets, spark);
		Dataset<Row> matches = MetaBlocking.metaBocking(spark, AliceDS, BobDS, blocks);
        matches.cache();
        printResult(matches);

//		Scanner myscanner = new Scanner(System.in);
//		myscanner.nextLine();
//		myscanner.close();

		spark.close();
	}

	private static void printResult(Dataset<Row> matches) {
		long matchesSize = matches.count();
		long tp =  matches.filter((FilterFunction<Row>) row -> row.getString(0).equals(row.getString(1))).count();
		long fp = matchesSize - tp ;

		System.out.println("Execution time: " + Timer.stop() + " seconds");
		long commons = (long) (Conf.DB_SIZE * Conf.COMMON_RECORDS);

		System.out.println(tp);
		System.out.println(matchesSize);
		System.out.println("Recall : " + (double) tp / commons );
		System.out.println("Precision : " + (double) tp / matchesSize );
	}
}