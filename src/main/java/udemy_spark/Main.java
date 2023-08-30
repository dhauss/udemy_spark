package udemy_spark;

import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		List<Double> inputData = new ArrayList<Double>();
		inputData.add(35.5);
		inputData.add(12.3);
		inputData.add(398.234);
		inputData.add(4.39);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf =  new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Double> myRDD = sc.parallelize(inputData);
		
		Double res = myRDD.reduce((val1, val2) -> val1 + val2);
		System.out.println(res);
		
		sc.close();
	}
}
