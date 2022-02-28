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
        
        SparkConf conf = new SparkConf().setAppName("Twitter LanguageFilte rApp");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        
        JavaRDD<String> data = sparkContext.textFile(inputDir);

        JavaRDD<String> tweets = data
            .flatMap(s -> Arrays.asList(s.split("[\n]")).iterator())
            .map(st -> SimplifiedTweet.fromJson(st))
            .filter(o -> o.isPresent())
            .map(t -> t.get())
            .filter(f -> f.getLanguage().equals(language))
            .map(s -> s.toString());				
        
        System.out.println("Language: "+language+"  Tweets: " + tweets.count());
        tweets.saveAsTextFile(outputDir);
        sparkContext.close();
       
    }

}

