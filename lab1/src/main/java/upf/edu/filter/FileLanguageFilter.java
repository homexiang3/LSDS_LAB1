package upf.edu.filter;


import upf.edu.parser.SimplifiedTweet;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Optional;
import com.google.gson.Gson;


public class FileLanguageFilter implements LanguageFilter {
	
	private final String inputFile;			  
	private final String outputFile;  		   

	public FileLanguageFilter(String inputFile, String outputFile) {

		 	this.inputFile = inputFile;
		    this.outputFile = outputFile;
		    
	}

	@Override
	public void filterLanguage(String language) throws Exception {
		
		FileReader reader = new FileReader(this.inputFile);
		BufferedReader bReader = new BufferedReader(reader);
		
		String line = bReader.readLine(); // Read one line of content
		
		SimplifiedTweet tweet = null;
		Optional <SimplifiedTweet> t = tweet.fromJson(line);
		Optional <String> tweetlang = t.map(SimplifiedTweet::getLanguage);
		
		if(t.isPresent() && language.equals(tweetlang)) {
			
			FileWriter writer = new FileWriter(this.outputFile);
			BufferedWriter bWriter = new BufferedWriter(writer);
			
			Gson gson = new Gson();
			String json = gson.toJson(t);
			
			bWriter.write(json); // Write one line of content
			bWriter.close(); // Close buffered writer and enclosed writer
		}
		
		bReader.close(); // Close buffered reader and enclosed reader
		
	}
}