package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import upf.edu.util.LanguageMapUtils;

import java.io.IOException;

public class TwitterWithWindow {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String input = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter with windows");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

     // read map.tsv by lines
        final JavaRDD<String> lines = jsc
                .sparkContext()
                .textFile(input);
        
        // add language map
        final JavaPairRDD<String, String> linesMap = LanguageMapUtils
                .buildLanguageMap(lines);

        //Language on tweets by descending order
       final JavaPairDStream<String,Integer> languageCount = stream
    		   .transformToPair(aux ->   aux.mapToPair(word -> new Tuple2 <> (word.getLang(),1))
               .join(linesMap)
               .mapToPair(x -> new Tuple2 <> (x._2._2,x._2._1))
               .reduceByKey((a,b) -> a+b))
    		   .mapToPair(Tuple2::swap)						
    		   .transformToPair(s ->s.sortByKey(false))		
    		   .mapToPair(Tuple2::swap);

        //Switch to display as count - language
        final JavaPairDStream<Integer, String> languageBatch = languageCount
        		.mapToPair(x -> new Tuple2<>(x._2,x._1)); 

        //Create window with top 15 lang in last minute
        final JavaPairDStream<Integer, String> languageWindow = languageBatch
        		.mapToPair(Tuple2::swap)
        		.reduceByKeyAndWindow((a,b) -> a+b, Durations.seconds(60))
        		.mapToPair(Tuple2::swap)
        		.transformToPair(s ->s.sortByKey(false));
        
        // Print first 15 results for each one
        languageBatch.print(15);
        languageWindow.print(15);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
