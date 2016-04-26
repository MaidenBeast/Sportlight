package prova;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
	public static void main(String[] args) {
		String logFile = "d:/Hadoop/prova.txt"; // Should be some file on your system
		
		//Inizializza Spark
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Carica tutto il contenuto del file in memoria
		JavaRDD<String> logData = sc.textFile(logFile).cache();
		long numAs = logData.filter(
				new Function<String, Boolean>() {
					public Boolean call(String s) {
						return s.contains("a");
					}
				}).count();
		long numBs = logData.filter(
				new Function<String, Boolean>() {
					public Boolean call(String s) {
						return s.contains("b");
					}
				}).count();
		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	}
}
