package upf.edu.model;

import java.util.Optional;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ExtendedSimplifiedTweet implements Serializable {
	private final long tweetId; // the id of the tweet (’id’)
	private final String text; // the content of the tweet (’text’)
	private final long userId; // the user id (’user->id’)
	private final String userName; // the user name (’user’->’name’)
	private final long followersCount; // the number of followers (’user’->’followers_count’)
	private final String language; // the language of a tweet (’lang’)
	private final boolean isRetweeted; // is it a retweet? (the object ’retweeted_status’ exists?)
	private final Long retweetedUserId; // [if retweeted] (’retweeted_status’->’user’->’id’)
	private final Long retweetedTweetId; // [if retweeted] (’retweeted_status’->’id’)
	private final long timestampMs; // seconds from epoch (’timestamp_ms’)
	
	public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName, long followersCount, String language, boolean isRetweeted,
	Long retweetedUserId, Long retweetedTweetId, long timestampMs) {
	// IMPLEMENT ME
		this.tweetId = tweetId;
	    this.text = text;
	    this.userId = userId;
	    this.userName = userName;
	    this.followersCount = followersCount;
	    this.language = language;    
	    this.isRetweeted = isRetweeted;
	    this.retweetedUserId = retweetedUserId;
	    this.retweetedTweetId = retweetedTweetId;
	    this.timestampMs = timestampMs;
	}
	
	/**
	* Returns a {@link ExtendedSimplifiedTweet} from a JSON String.
	* If parsing fails, for any reason, return an {@link Optional#empty()}
	*
	* @param jsonStr
	* @return an {@link Optional} of a {@link ExtendedSimplifiedTweet}
	*/
	public static Optional<ExtendedSimplifiedTweet> fromJson(String jsonStr) {
	// IMPLEMENT ME
		JsonElement je = parser.parse(jsonStr);
	    JsonObject jo = je.getAsJsonObject();

	    try {
	      long userId = jo.getAsJsonObject("user").get("id").getAsLong();
	      String userName = jo.getAsJsonObject("user").get("name").getAsString();

	      long tweetId = jo.get("id").getAsLong();
	      String text = jo.get("text").getAsString();
	      long  followersCount = jo.get("followersCount").getAsLong();
	      String language = jo.get("language").getAsString();
	      boolean isRetweeted = jo.get("isRetweeted").getAsBoolean();
	      
	      Long retweetedUserId = jo.get("retweetedUserId").getAsLong();
	      Long retweetedTweetId = jo.get("retweetedTweetId").getAsLong();
	      
	      long timestampMs = jo.get("timestamp_ms").getAsLong();
	      
	      ExtendedSimplifiedTweet tweet = new ExtendedSimplifiedTweet(tweetId, text, userId, userName, followersCount, language, isRetweeted, retweetedUserId, retweetedTweetId, timestampMs);
	      Optional<ExtendedSimplifiedTweet> t = Optional.ofNullable(tweet);

	      return t;
	    } finally {
	      return Optional.empty();
	    }
	}
}