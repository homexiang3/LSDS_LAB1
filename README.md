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

 
