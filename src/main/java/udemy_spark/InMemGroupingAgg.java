package udemy_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.List;
import java.util.ArrayList;


public class InMemGroupingAgg {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("GroupingsAndAggs")
				.master("local[*]")
				.getOrCreate();
		
		List<Row> inMem = new ArrayList<Row>();
		inMem.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMem.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMem.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMem.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMem.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));
		
		StructField[] fields = new StructField[] {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
		};

		StructType schema = new StructType(fields);
		Dataset<Row> dataset = spark.createDataFrame(inMem, schema);
		
		dataset.createOrReplaceTempView("logging_table");
		
		Dataset<Row> levelCount = spark.sql("select level, count(datetime) from logging_table group by level order by level");
		levelCount.show();
		
		spark.close();
	}

}
