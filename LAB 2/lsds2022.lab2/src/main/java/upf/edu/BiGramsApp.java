package upf.edu;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import upf.edu.model.ExtendedSimplifiedTweet;

import java.util.*;

public class BiGramsApp {
	public static String[] stopwords_en = {"httpstco", "the", "this", "of", "is", "a", "for", "so", "to", "in", "for", "at", "i", "am", "and", "if"};
	public static String[] stopwords_es = {"httpstco", "a", "en", "de", "la", "el", "un", "una", "y", "que", "ha", "nos", "por", "se", "me", "lo", "las"};
	public static String[] stopwords_fr = {"httpstco", "ce", "je", "de", "la", "el", "l", "j", "les", "le", "y", "a", "un"};
	
    public static void main(String[] args){
    	// If we pass a directory Spark processes it.
    	String language = args[0];
        String outputDir = args[1];
        String input = args[2];
        
        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("BiGrams App");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        
        // Load input
        JavaRDD<String> sentences = sparkContext.textFile(input);

        JavaPairRDD<String, Integer> filteredTweets = sentences
            .flatMap(s -> Arrays.asList(s.split("[\n]")).iterator())
            .map(word -> ExtendedSimplifiedTweet.fromJson(word))
            .filter(optional -> optional.isPresent())
            .filter(tweet -> !tweet.get().isRetweeted())
            .filter(tweet -> tweet.get().getLanguage().equals(language))
            .map(simplifiedTweet -> getNGrams(2, simplifiedTweet.get().getText(), language))
            .flatMap(bigrams -> bigrams.iterator())
            .mapToPair(bigram -> new Tuple2<>(bigram, 1))
            .reduceByKey((a, b) -> a + b); //JavaPairRDD<String, Integer>
        
        filteredTweets.saveAsTextFile(outputDir);
        sparkContext.close();
    }

    private static String normalise(String word) {
        return word.toLowerCase().replaceAll("[^a-z0-9#]", "");
    }
    
    /*
     * Function to sanitize the tokens.
     * First normalize each token
	 * Then if the token becomes empty (because was '...' and I not accept this as token, or is part of url), I not add to the new list
	 * Remove items generates a lot of problems!
     */
    private static List<String> sanitizeTokens(List<String> tokens, String lang){
    	List<String> sanitizedTokens = new ArrayList<>();
    	List<String> stopwords;
    	if(lang.equals("es"))
    		stopwords = Arrays.asList(stopwords_es);
    	else if(lang.equals("en"))
    		stopwords = Arrays.asList(stopwords_en);
    	else
    		stopwords = Arrays.asList(stopwords_fr);
    	for (int i = 0; i < tokens.size(); i++) {
    		String normalized_token = normalise(tokens.get(i));
    		if (normalized_token.isEmpty() || stopwords.contains(normalized_token))
    			continue;
    		else
    			sanitizedTokens.add(normalized_token);
    	}
    	return sanitizedTokens;
    }
    
    private static List<String> getNGrams(int n, String tweet, String lang) {
    	List<String> ngrams = sanitizeTokens(Arrays.asList(tweet.split(" ")), lang);
    	List<String> allNGrams =  new ArrayList<String>();
    	for (int i = 0; i < ngrams.size() - n + 1; i++) {
        	String nGram = "";
        	for (int j = 0; j < n; j++) {
        		String tmp = " " + ngrams.get(i+j);
            	nGram += tmp;
            }
        	
        	// I trim at the end, to remove the first space of the nGram.
        	allNGrams.add(nGram.trim());
        }
        return allNGrams;
    }
}
