package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import java.util.List;


import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;

import java.io.IOException;

public class TwitterWithState {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String language = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter With State");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // create a simpler stream of <user, count> for the given language
        final JavaPairDStream<String, Integer> tweetPerUser = stream
        		.filter(x -> x.getLang().equals(language))
                .mapToPair(x -> new Tuple2<>(x.getUser().getScreenName(),1))
                .reduceByKey((a,b)->a+b);
        
        // transform to a stream of <userTotal, userName> 
        final JavaPairDStream<Integer, String> tweetsCountPerUser = tweetPerUser
        		.updateStateByKey(updateFunction)
                .mapToPair(x->new Tuple2<>(x._2(),x._1()))
                .transformToPair(s -> s.sortByKey(false));
        
        //print first 20
        tweetsCountPerUser.print(20);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
    static Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
            (values, state) -> {
                int count = 0;
                if(state.isPresent()){
                    count = state.get();
                }
                for(int v: values){
                    count+=v;
                }

                return Optional.of(count);
            };

}
