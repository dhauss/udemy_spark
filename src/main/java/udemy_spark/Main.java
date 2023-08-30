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
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(398);
		inputData.add(4);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf =  new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> myRDD = sc.parallelize(inputData);
		
		Integer res = myRDD.reduce((val1, val2) -> val1 + val2);
		System.out.println("Sum: " + res);
		
		JavaRDD<Double> roots = myRDD.map(Math::sqrt);
		
		roots.foreach(root -> System.out.println(root));
		//roots.collect().forEach(System.out::println);
		
		System.out.println(roots.count());
		JavaRDD<Long> counter = myRDD.map(val -> 1L);
		Long countRes = counter.reduce((val1, val2) -> val1 + val2);
		System.out.println("Count: " + countRes);
		
		
		sc.close();
	}
}
