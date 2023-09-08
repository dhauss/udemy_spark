package udemy_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class SparkSQLSandbox {
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("TestingSQL")
				.master("local[*]")
				.getOrCreate();

		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		dataset.show();
		//System.out.println("Number of rows: " + dataset.count());

		/*  accessing fields and rows
		Row firstRow = dataset.first();
		String subject = firstRow.getString(2).toString();
		String grade = firstRow.getAs("grade").toString();
		int year = Integer.parseInt(firstRow.getAs("year"));
		System.out.println(subject + ": " + grade);
		System.out.println(year);
		*/

		//SQL style filtering, note single quotes for multi-word category
		Dataset<Row> modernArtSQL = dataset
				.filter("subject = 'Modern Art' AND year >= 2007");
		
		modernArtSQL.show();

		//column class approach. NOTE static import of functions to use col on second line of filter
		/*
		Column subjectColumn = dataset.col("subject");
		Dataset<Row> modernArtCol = dataset
				.filter(subjectColumn.equalTo("Modern Art").and(col("year").geq(2007)));
		modernArtCol.show();
		*/
		
		//temporary view, create in memory table to query with SQL statements
		dataset.createOrReplaceTempView("my_students_view");
		Dataset<Row> res = spark.sql("select score, year from my_students_view where subject = 'French' ");
		
		Dataset<Row> max = spark.sql("select max(score) from my_students_view where subject = 'French' ");
		max.show();
		
		Dataset<Row> years = spark.sql("select distinct(year) from my_students_view order by year desc");
		years.show();
		
		spark.close();
	};
}
