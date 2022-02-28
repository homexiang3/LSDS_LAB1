package upf.edu;


import upf.edu.model.SimplifiedTweet;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class TwitterLanguageFilterApp {
    public static void main( String[] args ) {
    	
    	String language = args[0];
        String outputDir = args[1];
        String inputDir = args[2];

        //Create a SparkContext to initialize
        
        SparkConf conf = new SparkConf().setAppName("Word Count");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        
        // Load input
        
        JavaRDD<String> input = sparkContext.textFile(inputDir);
        
		// Parse and filter
        
        JavaRDD<SimplifiedTweet> tweets = 
        		input.filter(t -> t.contains("created_at"))
        			 .map(t -> SimplifiedTweet.fromJson(t))
        			 .filter(o -> o.isPresent())
        			 .map(o -> o.get())
        			 .filter(tweet -> tweet.getLanguage().equals(language));
        tweets.saveAsTextFile(outputDir);
       
    }

}

