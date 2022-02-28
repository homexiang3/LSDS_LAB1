package upf.edu;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import upf.edu.model.ExtendedSimplifiedTweet;

import java.util.*;

public class MostRetweetedApp {
	
	
	public static void main(String[] args){
    	// If we pass a directory Spark processes it.
        String outputDir = args[0];
        String inputDir = args[1];
        
        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Most Retweeted App");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        
        // Load input
        JavaRDD<String> data = sparkContext.textFile(inputDir);
        
        JavaRDD<ExtendedSimplifiedTweet> tweets = data
        		.flatMap(s -> Arrays.asList(s.split("[\n]")).iterator())					
                .map(et -> ExtendedSimplifiedTweet.fromJson(et))			
                .filter(o -> o.isPresent())
                .map(t -> t.get())
                .filter(rtTweet -> rtTweet.isRetweeted());
        
        JavaPairRDD<Long, Integer> mostRtUsers = tweets
            .mapToPair(t -> new Tuple2<>(t.getRetweetedUserId(), 1))	
            .reduceByKey((a, b) -> a + b)
            .mapToPair(x -> x.swap())
            .sortByKey(false)
            .mapToPair(x -> x.swap());


        
        JavaPairRDD<Long, Integer> mostRtTweets = tweets
        		.mapToPair(t -> new Tuple2<>(t.getRetweetedTweetId(), 1))	
                .reduceByKey((a, b) -> a + b)
                .mapToPair(x -> x.swap())
                .sortByKey(false)
                .mapToPair(x -> x.swap());
        
        
        JavaPairRDD<Long, ExtendedSimplifiedTweet> userIdTweets = tweets
        		.mapToPair(t -> new Tuple2<>(t.getUserId(), t))	
                .mapToPair(x -> x.swap())
                .sortByKey(false)
                .mapToPair(x -> x.swap());
        
        
        //tweets.saveAsTextFile(outputDir);
        sparkContext.close();
    }    
}
