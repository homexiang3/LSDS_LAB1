package upf.edu;

import java.util.*;
import upf.edu.model.HashTagCount;
import upf.edu.storage.DynamoHashTagRepository;

public class TwitterHashtagsReader {

    public static void main(String[] args) throws Exception {
        String language = args[0];

        DynamoHashTagRepository db = new DynamoHashTagRepository();
        List<HashTagCount> top10= db.readTop10(language);

        //print top 10 hashtags in specified language
        
        System.out.println("Top 10 hashtags in language "+language);
        
        for(int i=0; i<top10.size(); i++)
            System.out.println("#"+top10.get(i).getHashTag()+" : "+top10.get(i).getCount());
    }
}