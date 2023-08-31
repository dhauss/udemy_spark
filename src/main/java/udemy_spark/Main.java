package udemy_spark;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

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

		JavaRDD<Integer> OGIntegers = sc.parallelize(inputData);
		JavaRDD<Double> roots = OGIntegers.map(Math::sqrt);
		JavaRDD<Tuple2<Integer, Double>> rootPairs = OGIntegers.map(val -> new Tuple2<>(val, Math.sqrt(val)));
		// roots.foreach(root -> System.out.println(root));
		//roots.collect().forEach(System.out::println);

		/*  Count function and vanilla count
		System.out.println(roots.count());
		JavaRDD<Long> counter = OGIntegers.map(val -> 1L);
		Long countRes = counter.reduce((val1, val2) -> val1 + val2);
		System.out.println("Count: " + countRes);
		 */

		List<String> logData = new ArrayList<>();
		logData.add("WARN: Tuesday 4 September 0405");
		logData.add("ERROR: Tuesday 4 September 0408");
		logData.add("FATAL: Wednesday 5 September 1632");
		logData.add("ERROR: Friday 7 September 1854");
		logData.add("WARN: Saturday 8 September 1942");
		
		/* save full log in tuple form
		JavaPairRDD<String, String> logPairs = sc.parallelize(logData).mapToPair(rawVal -> {
			String[] cols = rawVal.split(":");
			String level = cols[0].trim();
			String date = cols[1].trim();
			
			return new Tuple2<>(level, date);
		});
		*/

		//count log message instances
		JavaPairRDD<String, Long>  logInstances = sc.parallelize(logData).mapToPair(rawVal -> {
			String level = rawVal.split(":")[0];
			return new Tuple2<>(level, 1L);
		});

		logInstances.reduceByKey((val1, val2) -> val1 + val2)
			.foreach(tup -> System.out.println("Message: " + tup._1 + ", Count: " + tup._2));

		//Flat Map and Filter
		JavaRDD<String> sentences = sc.parallelize(logData);
		JavaRDD<String> words = sentences.flatMap(val -> Arrays.asList(val.split(" ")).iterator());

		JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);

		filteredWords.foreach(val -> System.out.println(val));
		sc.close();
	}
}
