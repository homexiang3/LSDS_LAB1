package upf.edu.filter;


import upf.edu.parser.SimplifiedTweet;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Optional;

public class FileLanguageFilter implements LanguageFilter {
	
	private final String inputFile;			  
	private final String outputFile;  		   

	public FileLanguageFilter(String inputFile, String outputFile) {

		 	this.inputFile = inputFile;
		    this.outputFile = outputFile;
		    
	}

	@Override
	public void filterLanguage(String language) throws Exception {
		
		FileReader reader = new FileReader("/some/file.txt");
		BufferedReader bReader = new BufferedReader(reader);
		
		String line = bReader.readLine(); // Read one line of content
		
		SimplifiedTweet a = new SimplifiedTweet(0, line, 0, line, line, 0);
		
		
		FileWriter writer = new FileWriter("/some/other/file.txt");
		BufferedWriter bWriter = new BufferedWriter(writer);
		
		bWriter.write(line); // Write one line of content
		bReader.close(); // Close buffered reader and enclosed reader
		bWriter.close(); // Close buffered writer and enclosed writer
	}
}