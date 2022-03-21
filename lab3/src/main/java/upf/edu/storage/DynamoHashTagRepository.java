package upf.edu.storage;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import upf.edu.model.HashTagCompare;
import upf.edu.model.HashTagCount;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import java.io.Serializable;
import java.util.*;

public class DynamoHashTagRepository implements IHashtagRepository, Serializable {
	
	final static String endpoint = "dynamodb.us-east-1.amazonaws.com";
	final static String region = "us-east-1";
	final static String tableName = "LSDS2020-TwitterHashtags";

	final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
			.withEndpointConfiguration(
					new AwsClientBuilder.EndpointConfiguration(endpoint, region)
			).withCredentials(new ProfileCredentialsProvider())
			.build();

	final DynamoDB dynamoDB = new DynamoDB(client);
	final Table dynamoDBTable = dynamoDB.getTable(tableName);
		
  @Override
  public void write(Status tweet) {
	  HashtagEntity[] entities = tweet.getHashtagEntities();
	    if (entities != null) {
	    	for (HashtagEntity he : entities) {
	    		if (he != null) {
	  				String hashtag = he.getText().toString();
	  				String lang = tweet.getLang().toString();
	  				Long tweetId = tweet.getId();
	  				try {
	  					//update query
		    			UpdateItemSpec updateItemSpec = new UpdateItemSpec()
		    					.withPrimaryKey("hashtag", hashtag, "language", lang)
		    		            .withUpdateExpression("set myCounter=myCounter+:b, tweetsId=list_append(:a,tweetsId)")
		    		            .withValueMap(new ValueMap().withNumber(":b",1).withList(":a", tweetId))
		    		            .withReturnValues(ReturnValue.UPDATED_NEW);
		    			
		    			//Update on db
		    			dynamoDBTable.updateItem(updateItemSpec);
		    			
		    			//print
		    			System.out.println("Updated Hashtag: "+hashtag);
		    				
	  				} catch(Exception e) {

		    			// Create new hashtag 
		    			Item item = new Item()
		    				.withPrimaryKey("hashtag", hashtag, "language", lang)
		    				.withNumber("myCounter", 1)
		                    .withList("tweetsId", Arrays.asList(tweetId));
		    				
		    			// Add to db
	  					dynamoDBTable.putItem(item);
	  					
	  					//print
	  					System.out.println("New Hashtag: "+hashtag);
	  				}
	    		}
	    	}
	    }
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public List<HashTagCount> readTop10(String lang) {
	  ArrayList<HashTagCount> languageList = new ArrayList<HashTagCount>();
      ArrayList<HashTagCount> top10 = new ArrayList<HashTagCount>();

      //Scan db
      ScanRequest scanRequest = new ScanRequest()
              .withTableName(tableName)
              .withAttributesToGet("hashtag","language","myCounter");
      ScanResult result = client.scan(scanRequest);

      //Parse items
      for (Map<String, AttributeValue> item : result.getItems()) {
            String counter = item.values().toArray()[0].toString().split("[ ]")[1].split("[,]")[0];
            String language = item.values().toArray()[1].toString().split("[ ]")[1].split("[,]")[0];
            String hashtag = item.values().toArray()[2].toString().split("[ ]")[1].split("[,]")[0];
            
            //Language filter
            if(language.equals(lang)){
              HashTagCount h = new HashTagCount(hashtag, language, Long.parseLong(counter));
              languageList.add(h);
            }
      }
      
      //Rank list 
      Collections.sort(languageList,new HashTagCompare());
      
      //Get first 10 elements of list
      @SuppressWarnings("rawtypes")
      Iterator itr=languageList.iterator();
      int i=0;
      while(itr.hasNext() && i<10){
              top10.add((HashTagCount)itr.next());
              i++;
      }
      
      return top10;
  }

}
