package upf.edu.uploader;

import java.util.List;

import com.amazonaws.services.s3.AmazonS3;

public class S3Uploader implements Uploader {
	
	
	private final String bucket;
	private final String prefix;
	private final AmazonS3 Client;
	
	public S3Uploader(String bucket, String prefix, AmazonS3 Client) {

		this.bucket = bucket;
		this.prefix = prefix;
		this.Client = Client;
	    
	}
	public boolean bucketExists(String bucket) {
		//check if exists TO DO
		return true;
	}

	@Override
	public void upload(List<String> files) {
		// TODO Auto-generated method stub
		
	}

}
