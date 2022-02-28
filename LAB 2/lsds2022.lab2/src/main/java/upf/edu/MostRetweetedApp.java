package upf.edu;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import upf.edu.model.ExtendedSimplifiedTweet;

import java.util.*;

public class MostRetweetedApp {
	
	static List<Long> top20RtUsers =  Arrays.asList(new Long[] {3143260474L, 24679473L, 15584187L, 437025093L, 39538010L, 38381308L, 739812492310896640L, 1501434991L, 29056256L, 2754746065L, 3260160764L, 93618138L, 2424309080L, 1000011770L, 62513246L, 1290550819L, 185702760L, 2882361877L, 1264023596L, 444070452L});
	static List<Long> tweetsOfTop20Users =  Arrays.asList(new Long[] {995356756770467840L, 995435123351973890L, 995381560277979136L, 995406052190445568L, 995417089656672256L, 995384555719839744L, 995451073606406144L, 995388604045316097L, 995405811261300740L, 995439842107576321L, 995397243426541568L, 995260351385161728L, 995433967351283712L, 995408001480654848L, 995407380358852608L, 995394150978727936L, 995417631820828673L, 995414104025247745L, 995434971765538817L, 995383476181401601L, 995423337856806912L, 995333676614475776L, 995440696197697536L, 995392726798565376L, 995427353231839232L, 995448259962339329L, 995402237831712768L, 995396173623189505L, 995434505463791616L, 995433742771552256L, 995434692051599361L, 995382926018859008L, 995372692902699009L, 995447721791315969L, 995402023859294209L, 995420352657461248L, 995371907301208064L, 995387721324683265L, 995433175584329729L, 995414446867730433L, 994965210283769857L, 995397610927263745L, 995224986242646016L, 995393892697739265L, 995389193294635008L, 995403035047333889L, 995384731805118464L, 995389634694799360L, 995384060137488385L, 995379361825132544L, 995378197477736448L, 995372911920893953L, 995389248370040837L, 995397430685454337L, 995409866070999040L, 995391247278903296L, 995435431758938112L, 995410200256491526L, 995410782824230912L, 995388409958060033L, 995332906196357121L, 995422386559299590L, 995389672133218305L, 995435559454523392L, 995406419682721792L, 995409506166337538L, 995441101153550336L, 995403657851146244L, 995411642035900417L, 995436923916177408L, 995392098357710848L, 995420239671255040L, 995409216578949120L, 995260727719092224L, 995435102804070400L, 995418017134759936L, 995416742250807297L, 995382364753879045L, 995412178353172480L, 995434559436181504L, 995425380482809857L, 995412998532591616L, 995413535973928960L, 995404852158222336L, 995400913899671553L, 995389395128733697L, 995397072135360512L, 995371853354012672L, 972819536788508673L, 995406418898505728L, 995461341375758336L, 995384906225250304L, 995423384380026881L, 995384061899214848L, 995394238211911680L, 995384402841620481L, 995384065602793473L, 995420127611961344L, 995399484862590979L, 995388732973993990L, 995391158846218240L, 995407964319223808L, 994670844608696326L, 995400754407067648L, 994664507287928833L, 995383119791484928L, 995392114413391872L, 995377262047449088L, 995397298300563459L, 995407778234748928L, 995409514844315654L, 995381526476083201L, 995409928608153600L, 995425294298370048L, 995399792535666689L, 995393051995660290L, 995383408783122432L, 995408080354578432L, 995388112833531905L, 995408545200984064L, 995408052886147079L, 995397661355212800L, 995216336069562369L, 995406080464257024L, 995365102734921728L, 994674500611567617L, 995398078621536256L, 995377836272824320L, 995390332786229248L, 995387644577304576L, 995385645794242561L, 993935627249954816L, 995372754017730560L, 995420106250272768L, 995384654919237632L, 995386132941672449L, 995408855751311360L, 995398011286097921L, 995432295447302144L, 995389288916406272L, 995391380246720512L, 995387732724736000L, 993936992575606784L, 995424300206305281L, 995399561689493504L, 995393988684406784L, 995383653114040320L, 995442639716605952L, 995425815520251906L, 995419169171517440L, 995403052696907777L, 995401598275899392L, 995395816058695688L, 995441592616960000L, 995437742703239169L, 995410560656097280L, 995410495497744384L, 995386902579695616L, 995384817066881026L, 995441840496304128L, 995410105352048642L, 995409047607173120L, 995402871616229376L, 995424597452513287L, 995399343929733120L, 995383805883047936L, 995419770735484928L, 995411229781954561L, 995430909896740864L, 995412654511546368L, 995436506234966017L, 995407543425003522L, 995388519429402630L, 995379832337977344L, 994663885146873862L, 995416736978567169L, 995396880744890368L, 995395586210779136L, 995388119829577728L, 995434319379185664L, 994669080933265411L, 995404190112518146L, 995394585286209536L, 995398609536405504L, 995382109089890309L, 995381580045533185L, 995405131591159808L, 995396886197501952L, 995391838168154113L, 995403238630359043L, 995388413976035328L, 995380582954631169L, 995415057352876032L, 995387127708954624L, 995398577617866753L, 995420019189219331L, 995405639517011968L, 995401360559525888L, 995397273881325569L, 993938026429534210L});
	static List<Long> originalTweets =  Arrays.asList(new Long[] {995405131591159808L, 995406418898505728L, 995407543425003522L, 995408001480654848L, 995408052886147079L, 995409216578949120L, 995409928608153600L, 995411229781954561L, 995412654511546368L, 995413535973928960L, 995396880744890368L, 995399484862590979L, 995371853354012672L, 995371907301208064L, 995372692902699009L, 995372754017730560L, 995372911920893953L, 995377262047449088L, 995377836272824320L, 995378197477736448L, 995379361825132544L, 995379832337977344L, 995380582954631169L, 995381526476083201L, 995381580045533185L, 995382109089890309L, 995382364753879045L, 995383119791484928L, 995383408783122432L, 995383476181401601L, 995384654919237632L, 995384817066881026L, 995388112833531905L, 995388519429402630L, 995389193294635008L, 995441592616960000L, 995420352657461248L, 995422386559299590L, 995424597452513287L, 995427353231839232L, 995448259962339329L, 995451073606406144L, 995461341375758336L, 995332906196357121L, 995333676614475776L, 995356756770467840L, 995365102734921728L, 995436923916177408L, 995437742703239169L});
	
	public static void main(String[] args){
    	// If we pass a directory Spark processes it.
        String outputDir = args[0];
        String input = args[1];
        
        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Most Retweeted App");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        
        // Load input
        JavaRDD<String> sentences = sparkContext.textFile(input);
        
        JavaPairRDD<Long, Integer> mostRtUsers = sentences
            .flatMap(s -> Arrays.asList(s.split("[\n]")).iterator())					// split the input by newlines (tweets)
            .map(tweet_JSON -> ExtendedSimplifiedTweet.fromJson(tweet_JSON))			// Parse the JSON to ExtendedSimplifiedTweet
            .filter(optional -> optional.isPresent())									// Discard optional empty
            .filter(rtTweet -> rtTweet.get().getIsRetweeted())								// Discard Original tweets (NO retweeted)
            .mapToPair(rtTweet -> new Tuple2<>(rtTweet.get().getRetweetedUserId(), 1))	// Create tuples (retweeted_userId, 1)
            .reduceByKey((a, b) -> a + b);												// Reduced (retweeted_userId, sum)

        mostRtUsers.saveAsTextFile(outputDir+"/topRtUsers");

        
        JavaPairRDD<Long, Integer> tweetsOfTop20RtUsers = sentences
                .flatMap(s -> Arrays.asList(s.split("[\n]")).iterator())							// split the input by endline (tweets)
                .map(tweet_JSON -> ExtendedSimplifiedTweet.fromJson(tweet_JSON))					// Parse the JSON to ExtendedSimplifiedTweet
                .filter(optional -> optional.isPresent())											// Discard optional empty
                .filter(rtTweet -> rtTweet.get().getIsRetweeted())										// Discard Original tweets (NO retweeted)
                .filter(tweet ->  top20RtUsers.contains(tweet.get().getRetweetedUserId()))			// Discard retweets from NO TOP20RT Users
                .mapToPair(rtTweet -> new Tuple2<>(rtTweet.get().getRetweetedTweetId(), 1))			// Create tuples (retweeted_tweetId, 1)
		        .reduceByKey((a, b) -> a + b);														// Reduce by Reduced (retweeted_tweetId, sum)
		        
        tweetsOfTop20RtUsers.saveAsTextFile(outputDir+"/tweetsOfTop20RtUsers");
        
        JavaRDD<Long> tweets2 = sentences
                .flatMap(s -> Arrays.asList(s.split("[\n]")).iterator())							// split the input by endline (tweets)
                .map(word -> ExtendedSimplifiedTweet.fromJson(word))								// Parse the JSON to ExtendedSimplifiedTweet
                .filter(optional -> optional.isPresent())											// Discard optional empty
                .filter(rtTweet -> !rtTweet.get().getIsRetweeted())									// Discard retweeted tweets
                .filter(tweet -> tweetsOfTop20Users.contains(tweet.get().getTweetId()))				// Discard any tweet that not are from the Top20 retweeted Users
                .map(rtTweet -> rtTweet.get().getTweetId());										// Create RDD<String> with the TweetId of these tweets
                
        tweets2.saveAsTextFile(outputDir+"/originalTweetsID");
        
        
        JavaRDD<String> tweets3 = sentences
        		.flatMap(s -> Arrays.asList(s.split("[\n]")).iterator())							// split the input by endline (tweets)
                .map(word -> ExtendedSimplifiedTweet.fromJson(word))								// Parse the JSON to ExtendedSimplifiedTweet
                .filter(optional -> optional.isPresent())											// Discard optional empty
                .filter(rtTweet -> rtTweet.get().getIsRetweeted())										// Discard Original tweets (NO retweeted)
		        .filter(tweet -> originalTweets.contains(tweet.get().getRetweetedTweetId()))		// Discard any retweet that not are from the original Tweets
		        .map(rtTweet -> (rtTweet.get().getRetweetedTweetId()+" "+rtTweet.get().getRetweetedUserId()));		// Create RDD<String> with the RetweetedTweetID and getRetweetedUserID
        
        tweets3.saveAsTextFile(outputDir+"/originalTweets_top20Users");
        
        sparkContext.close();
    }    
}
