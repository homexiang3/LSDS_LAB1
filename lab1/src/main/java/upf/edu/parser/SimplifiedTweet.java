package upf.edu.parser;

//import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
//import jdk.nashorn.internal.parser.JSONParser;

import java.io.Serializable;
import java.util.Optional;

public class SimplifiedTweet {

  private static JsonParser parser = new JsonParser();



  private final long tweetId;			  // the id of the tweet ('id')
  private final String text;  		      // the content of the tweet ('text')
  private final long userId ;			  // the user id ('user->id')
  private final String userName;		  // the user name ('user'->'name')
  private final String language;          // the language of a tweet ('lang')
  private final long timestampMs;		  // seconduserIds from epoch ('timestamp_ms')

  public SimplifiedTweet(long tweetId, String text, long userId, String userName,
                         String language, long timestampMs) {

    this.tweetId = tweetId;
    this.text = text;
    this.userId = userId;
    this.userName = userName;
    this.language = language;
    this.timestampMs = timestampMs;

  }
  

  public static JsonParser getParser() {
	return parser;
  }


  public static void setParser(JsonParser parser) {
	SimplifiedTweet.parser = parser;
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


/**
   * Returns a {@link SimplifiedTweet} from a JSON String.
   * If parsing fails, for any reason, return an {@link Optional#empty()}
   *
   * @param jsonStr
   * @return an {@link Optional} of a {@link SimplifiedTweet}
   */
  public static Optional<SimplifiedTweet> fromJson(String jsonStr) {

    // PLACE YOUR CODE HERE!

    JsonElement je = parser.parse(jsonStr);
    JsonObject jo = je.getAsJsonObject();
    
    try {
    long userId = jo.getAsJsonObject("user").get("id").getAsLong();
    String userName = jo.getAsJsonObject("user").get("name").getAsString(); 
    
    long tweetId = jo.get("id").getAsLong();
    String text = jo.get("text").getAsString();
    String language = jo.get("language").getAsString();
    long timestampMs = jo.get("timestamp_ms").getAsLong();
    
    SimplifiedTweet tweet = new SimplifiedTweet(tweetId,text,userId,userName,language,timestampMs);
    Optional<SimplifiedTweet> t = Optional.ofNullable(tweet);
    
    return t;
    }finally{
    	return Optional.empty();
    }

  }


  @Override
  public String toString() {
    return "SimplifiedTweet{" +
            "tweetId=" + tweetId +
            ", text='" + text + '\'' +
            ", userId=" + userId +
            ", userName='" + userName + '\'' +
            ", language='" + language + '\'' +
            ", timestampMs=" + timestampMs +
            '}';
  }
}
