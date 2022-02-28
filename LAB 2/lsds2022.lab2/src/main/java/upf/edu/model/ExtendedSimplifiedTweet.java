package upf.edu.model;

import java.util.Optional;
import java.io.Serializable;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ExtendedSimplifiedTweet implements Serializable {
	
	private static JsonParser parser = new JsonParser();
	
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
	
	public long getFollowersCount() {
		return followersCount;
	}

	public boolean isRetweeted() {
		return isRetweeted;
	}

	public Long getRetweetedUserId() {
		return retweetedUserId;
	}

	public Long getRetweetedTweetId() {
		return retweetedTweetId;
	}

	
	public long getTweetId() {
		return tweetId;
	}
	

	public String getText() {
		return text;
	}
	
	public long getUserId() {
		return userId;
	}
	
	public String getUserName() {
		return userName;
	}
	
	public String getLanguage() {
		return language;
	}
	
	public long getTimestampMs() {
		return timestampMs;
	}



	public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName,
						long followersCount, String language, boolean isRetweeted, long retweetedUserId,
						long retweetedTweetId, long timestampMs) {
		
		this.tweetId=tweetId;
		this.text=text;
		this.userId=userId;
		this.userName=userName;
		this.followersCount = followersCount;
		this.language=language;
		this.isRetweeted = isRetweeted;
		this.retweetedUserId = retweetedUserId;
		this.retweetedTweetId = retweetedTweetId;
		this.timestampMs=timestampMs;	
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

		  try {
				JsonElement je = parser.parse(jsonStr);
				JsonObject json = je.getAsJsonObject();
				
				long tweetId = json.get("id").getAsLong();
				String text = json.get("text").getAsString();
				long userId = json.get("user").getAsJsonObject().get("id").getAsLong(); 
				String userName = json.get("user").getAsJsonObject().get("name").getAsString();
				long followersCount = json.get("user").getAsJsonObject().get("followers_count").getAsLong(); 
				boolean isRetweeted = json.has("retweeted_status");
				
				long retweetedTweetId, retweetedUserId;
				if(isRetweeted) {
					retweetedTweetId = Optional
							.ofNullable(json.get("retweeted_status"))
							.map(JsonElement::getAsJsonObject)
							.map(jo -> jo.get("id"))
							.map(JsonElement::getAsLong)
							.orElse(null);
					
					Optional<JsonObject> optionalUser = Optional
							.ofNullable(json.get("retweeted_status"))
							.map(JsonElement::getAsJsonObject)
							.map(jo -> jo.get("user"))
							.map(JsonElement::getAsJsonObject);
					retweetedUserId = optionalUser
							.map(jo -> jo.get("id"))
							.map(JsonElement::getAsLong)
							.orElse(null);

				} else {
					retweetedTweetId  = -1;
					retweetedUserId = -1; 
				}
				String language = json.get("lang").getAsString();
				long timestampMs = json.get("timestamp_ms").getAsLong();
		      
		      ExtendedSimplifiedTweet tweet = new ExtendedSimplifiedTweet(tweetId, text, userId, userName, followersCount, language, isRetweeted, retweetedUserId, retweetedTweetId, timestampMs);
		      Optional<ExtendedSimplifiedTweet> t = Optional.ofNullable(tweet);
	
		      return t;
		      
		    } catch(Exception ise){
		    	
		        return Optional.empty();
		        
		    }
	  }
	  @Override
	  public String toString() {
		  return new Gson().toJson(this);
	  }
}