package upf.edu;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import upf.edu.model.ExtendedSimplifiedTweet;

import java.util.*;

public class BiGramsApp {

    public static void main(String[] args){
    	// If we pass a directory Spark processes it.
    	String language = args[0];
        String outputDir = args[1];
        String inputDir = args[2];
        
        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("BiGrams App");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        
        // Load input
        JavaRDD<String> data = sparkContext.textFile(inputDir);

        JavaPairRDD<String, Integer> tweets = data
            .flatMap(s -> Arrays.asList(s.split("[\n]")).iterator())
            .map(word -> ExtendedSimplifiedTweet.fromJson(word))
            .filter(optional -> optional.isPresent())
            .filter(tweet -> !tweet.get().isRetweeted())
            .filter(tweet -> tweet.get().getLanguage().equals(language))
            .map(simplifiedTweet -> getNGrams(2, simplifiedTweet.get().getText()))
            .flatMap(bigrams -> bigrams.iterator())
            .mapToPair(bigram -> new Tuple2<>(bigram, 1))
            .reduceByKey((a, b) -> a + b)
            .mapToPair(x -> x.swap())
            .sortByKey(false)
            .mapToPair(x -> x.swap());
            
        
        tweets.saveAsTextFile(outputDir);
        sparkContext.close();
    }

    private static String normalise(String word) {
        return word.trim().toLowerCase();
    }
    

    private static List<String> sanitizeTokens(List<String> tokens){
    	List<String> sanitizedTokens = new ArrayList<>();
    	for (int i = 0; i < tokens.size(); i++) {
    		String normalized_token = normalise(tokens.get(i));
    		if (normalized_token.isEmpty())
    			continue;
    		else
    			sanitizedTokens.add(normalized_token);
    	}
    	return sanitizedTokens;
    }
    
    private static List<String> getNGrams(int n, String tweet) {
    	List<String> ngrams = sanitizeTokens(Arrays.asList(tweet.split(" ")));
    	List<String> allNGrams =  new ArrayList<String>();
    	for (int i = 0; i < ngrams.size() - n + 1; i++) {
        	String nGram = "";
        	for (int j = 0; j < n; j++) {
        		String tmp = " " + ngrams.get(i+j);
            	nGram += tmp;
            }
        	allNGrams.add(nGram.trim());
        }
        return allNGrams;
    }
}
