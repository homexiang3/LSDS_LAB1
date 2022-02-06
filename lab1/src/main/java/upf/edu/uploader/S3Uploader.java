package upf.edu.uploader;

import java.io.File;
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
		
		if (Client.doesBucketExistV2(this.bucket)) {
			return true;
		}else {
			return false;
		}
	}

	@Override
	public void upload(List<String> files) {
	
		if(!bucketExists(this.bucket)) {
			Client.createBucket(this.bucket);
		}
		for(int i = 0; i<files.size();i++) {
			Client.putObject(this.bucket, this.prefix, new File(files.get(i)));
		}
		
	}

}
