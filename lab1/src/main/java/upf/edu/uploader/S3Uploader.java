package upf.edu.uploader;

import java.util.List;

public class S3Uploader implements Uploader {
	
	
	private final String bucketName;
	private final String prefix;
	private final String credentialsProfileName;
	
	public S3Uploader(String bucketName, String prefix, String credentialsProfileName) {

		this.bucketName = bucketName;
		this.prefix = prefix;
		this.credentialsProfileName = credentialsProfileName;
	    
	}

	@Override
	public void upload(List<String> files) {
		// TODO Auto-generated method stub
		
	}

}
