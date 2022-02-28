package upf.edu.model;

import com.google.gson.Gson;
//import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
//import jdk.nashorn.internal.parser.JSONParser;

import java.io.Serializable;
import java.util.Optional;

import org.apache.spark.api.java.JavaRDD;

public class SimplifiedTweet {

  private static JsonParser parser = new JsonParser();

  private final long tweetId; // the id of the tweet ('id')
  private final String text; // the content of the tweet ('text')
  private final long userId; // the user id ('user->id')
  private final String userName; // the user name ('user'->'name')
  private final String language; // the language of a tweet ('lang')
  private final long timestampMs; // seconduserIds from epoch ('timestamp_ms')

  public SimplifiedTweet(long tweetId, String text, long userId, String userName,
      String language, long timestampMs) {

    this.tweetId = tweetId;
    this.text = text;
    this.userId = userId;
    this.userName = userName;
    this.language = language;
    this.timestampMs = timestampMs;

  }


  public String getLanguage() {
    return this.language;
  }


  /**
   * Returns a {@link SimplifiedTweet} from a JSON String.
   * If parsing fails, for any reason, return an {@link Optional#empty()}
   *
   * @param jsonStr
   * @return an {@link Optional} of a {@link SimplifiedTweet}
   */
  public static Optional<SimplifiedTweet> fromJson(String jsonStr) {


    try {
    	
      JsonElement je = parser.parse(jsonStr);
      JsonObject jo = je.getAsJsonObject();
      
      long userId = jo.getAsJsonObject("user").get("id").getAsLong();
      String userName = jo.getAsJsonObject("user").get("name").getAsString();

      long tweetId = jo.get("id").getAsLong();
      String text = jo.get("text").getAsString();
      String language = jo.get("lang").getAsString();
      long timestampMs = jo.get("timestamp_ms").getAsLong();

      SimplifiedTweet tweet = new SimplifiedTweet(tweetId, text, userId, userName, language, timestampMs);
      Optional<SimplifiedTweet> t = Optional.ofNullable(tweet);

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