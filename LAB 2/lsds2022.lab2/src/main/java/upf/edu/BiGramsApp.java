package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class BiGramsApp {

	public static void main(String[] args) {
		
		String language = args[0];
        String outputDir = args[1];
        String inputDir = args[2];
        
        //Create a SparkContext to initialize
        
        SparkConf conf = new SparkConf().setAppName("BiGramsApp");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        // Load input
        JavaRDD<String> input = sparkContext.textFile(inputDir);
        
		// Parse and filter TO DO

	}

}
