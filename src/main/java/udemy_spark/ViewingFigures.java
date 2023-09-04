package udemy_spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		Logger.getLogger("org").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data.
		boolean testMode = true;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);
		
		viewData = viewData.distinct();
		
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> chapterUserCourse = viewData
				.mapToPair(row -> new Tuple2<>(row._2, row._1))
				.join(chapterData);
		
		JavaPairRDD<Integer, Long> chapCountByUser = chapterUserCourse
				.mapToPair(row -> {
				Tuple2<Integer, Integer> userCourseTup = new Tuple2<>(row._2._1, row._2._2);
				return new Tuple2<Tuple2<Integer, Integer>, Long>(userCourseTup, 1L);
				})
				.reduceByKey((val1, val2) -> val1 + val2)
				.mapToPair(entry -> new Tuple2<>(entry._1._2, entry._2));
		
		JavaPairRDD<Integer, Integer> chapterCount = chapterData
				.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, 1))
				.reduceByKey((val1, val2) -> val1 + val2);
		
		JavaPairRDD<Integer, Double> chapCountPercent = chapCountByUser
				.join(chapterCount)
				.mapToPair(entry -> new Tuple2<>(entry._1, (double) entry._2._1/entry._2._2));
		
		JavaPairRDD<Integer, Long> finalScores = chapCountPercent
				.mapValues(val -> {
					if(val > .9) return 10L;
					else if(val > .5) return 4L;
					else if(val > .25) return 2L;
					else return 0L;
				})
				.reduceByKey((val1, val2) -> val1 + val2);
		
		JavaPairRDD<Integer, Tuple2<Long, String>> addTitles = finalScores
				.join(titlesData);
		
		JavaPairRDD<Long, String> prettyOut = addTitles
				.mapToPair(entry -> new Tuple2<Long, String>(entry._2._1, entry._2._2));
		
		prettyOut.sortByKey(false).collect().forEach(entry ->
			System.out.println(entry._2 + ": " + entry._1));
		
		

				
		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
