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
    public static void main( String[] args ) throws Exception {
    	
    	String language = args[0];
        String outputDir = args[1];
        String inputDir = args[2];

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Word Count");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        // Load input
        JavaRDD<String> tweets = sparkContext.textFile(inputDir);
        
        SimplifiedTweet tweet = null;
		/*Optional <SimplifiedTweet> t = tweet.fromJson(tweets);
		String tweetlang = t.map(SimplifiedTweet::getLanguage).orElse(null);
		
		if(t.isPresent()) {
			
		}*/
		

        /*JavaPairRDD<String, Integer> counts = tweets
            .flatMap(s -> Arrays.asList(s.split("[ ]")).iterator())
            .map(word -> normalise(word))
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((a, b) -> a + b);
        System.out.println("Total words: " + counts.count());
        counts.saveAsTextFile(outputDir);*/
       
    }
    private static String normalise(String word) {
        return word.trim().toLowerCase();
    }
}

