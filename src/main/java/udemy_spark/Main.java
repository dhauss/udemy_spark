package udemy_spark;

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
		Logger.getLogger("org").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("EMRCluster");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initRDD = sc.textFile("s3://udemysparkbuck/input.txt");
		
		JavaRDD<String> sentencesOnly = initRDD
				.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
					.filter(sentence -> sentence.trim().length() > 0);
			
			JavaRDD<String> wordsOnly = sentencesOnly
					.flatMap(val -> Arrays.asList(val.split(" ")).iterator())
					.filter(word -> word.trim().length() > 0);
					
			JavaRDD<String> interestingWords = wordsOnly.filter(word -> Util.isNotBoring(word));
					
			JavaPairRDD<String, Long> wordCountTotals = interestingWords
					.mapToPair(word -> new Tuple2<String, Long>(word, 1L))
					.reduceByKey((val1, val2) -> val1 + val2);
			
			JavaPairRDD<Long, String> totalsSwitchedSorted = wordCountTotals
					.mapToPair(tup -> new Tuple2<Long, String>(tup._2, tup._1))
					.sortByKey(false);
			
			totalsSwitchedSorted.take(10).forEach(System.out::println);

	}

}
