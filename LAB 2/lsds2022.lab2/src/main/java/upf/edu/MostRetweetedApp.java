package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import upf.edu.model.ExtendedSimplifiedTweet;

public class MostRetweetedApp {

	public static void main(String[] args) {
		
		String language = args[0];
        String outputDir = args[1];
        String inputDir = args[2];
        
        //Create a SparkContext to initialize
        
        SparkConf conf = new SparkConf().setAppName("MostRetweetedApp");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        
        // Load input
        
        JavaRDD<String> input = sparkContext.textFile(inputDir);
        
		// Parse and filter TO DO
        JavaRDD<ExtendedSimplifiedTweet> tweets = input
        		.map(line -> ExtendedSimplifiedTweet.fromJson(line))
        		.filter(elem -> elem.isPresent())
        		.map(line -> line.get())
        		.filter(elem -> elem.getLanguage().equals(language))
        		.filter(elem -> elem.getIsRetweeted());
        
        /*JavaPairRDD<Long, Integer> rtUsers = tweets
        		.map(elem -> elem.getRetweetedUserId())
        		.mapToPair(elem -> new Tuple2<>(elem,1))
        		.reduceByKey((a,b) -> a + b)
        		.mapToPair(s -> s.swap())
        		.sortByKey(false)
        		.mapToPair(s -> s.swap());
        
        JavaPairRDD<Long,Integer> rtTweets = tweets
        		.map(elem -> elem.getRetweetedTweetId())
        		.mapToPair(elem -> new Tuple2<>(elem,1))
        		.reduceByKey((a,b) -> a + b)
        		.mapToPair(s -> s.swap())
        		.sortByKey(false)
        		.mapToPair(s -> s.swap());
        
        JavaPairRDD<Long, ExtendedSimplifiedTweet> rddint = tweets
        		.mapToPair(elem -> new Tuple2<>(elem.getUserId(),elem))
        		.mapToPair(s -> s.swap())
        		.sortByKey(false)
        		.mapToPair(s -> s.swap());*/
        
        //JavaPairRDD<long, ExtendedSimplifiedTweet> relation = rrdint.join(rtUsers)...
        
        tweets.saveAsTextFile(outputDir);

	}

}
