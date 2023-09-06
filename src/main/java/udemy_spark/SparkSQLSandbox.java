package udemy_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkSQLSandbox {
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
		//SparkConf conf =  new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		//JavaSparkContext sc = new JavaSparkContext(conf);
		
		SparkSession spark = SparkSession.builder().appName("TestingSQL").master("local[*]");
	};
}
