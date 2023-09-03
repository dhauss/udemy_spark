package udemy_spark;

import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class JoinSandbox {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("JoinSandbox").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
		visitsRaw.add(new Tuple2<>(4, 18)); 
		visitsRaw.add(new Tuple2<>(6, 4)); 
		visitsRaw.add(new Tuple2<>(10, 9)); 
		
		List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
		usersRaw.add(new Tuple2<>(1, "John"));
		usersRaw.add(new Tuple2<>(2, "Bob")); 
		usersRaw.add(new Tuple2<>(3, "Alan")); 
		usersRaw.add(new Tuple2<>(4, "Doris")); 
		usersRaw.add(new Tuple2<>(5, "Marybelle")); 
		usersRaw.add(new Tuple2<>(6, "Raquel")); 
		
		JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
		JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);
		
		//inner join
		JavaPairRDD<Integer, Tuple2<Integer, String>> userVisits = visits.join(users);
		
		userVisits.foreach(entry -> System.out.println(entry));

		
		
		sc.close();





	}

}
