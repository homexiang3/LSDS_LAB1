# LSDS_LAB1 BENCHMARKS
For benchmarks we decided to implement the function System.currentTimeMillis() at the begining of our main, store it in a variable "start" and then perform (System.currentTimeMillis()- "start") at the end

Benchmark for language "hu":
	- # tweets = 101
	- time in miliseconds = 69296
	
Benchmark for language "es":
	- # tweets = 169659
	- time in miliseconds = 91190
	
Benchmark for language "pt":
	- # tweets = 16516
	- time in miliseconds = 77168
	
Notice that since we are managing a lot of data in a sequential way our running time is high, despite there are a lot of difference on #tweets, we need a lot of time to process each of the json files.

# LSDS_LAB1 EXTENSIONS
For extensions we decided to create the 3 proposed junit test inside src/test/java/upf.edu.TweetFilterTest

Extension 1 parseValidTweet: we use a test json string with all the required fields, our parse should be pass without problems.
Extension 2 parseInvalidTweet: we use a test json string malfored (we decided to remove the first bracket), our parse should fail and we have the error malformedJsonException. Nevertheless test pass since we expect an Optional.empty();
Extension 3 ParseValidTweetMissing: we use a test json string with one missing field (id), our parse should fail and we have the error that the field is null. Nevertheless test pass since we expect an Optional.empty();

 
# Troubleshooting
the output file is updated on https://lsds-p102-06.s3.amazonaws.com/prefix, since we have a trouble that shows "Acces denied" if we don't change the file permissions to public,we decided to add a bucket policy public in order to test the code.