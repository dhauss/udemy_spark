package udemy_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLSandbox {
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("TestingSQL")
				.master("local[*]")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		dataset.show();
		System.out.println(dataset.count());
		
		spark.close();
	};
}
