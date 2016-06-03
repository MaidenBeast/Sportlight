package it.uniroma3.radeon.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamLoader {
	
	private Duration streamWindowSecs;
	private String appName;
	
	public StreamLoader(Integer window, String appName) {
		this.streamWindowSecs = Durations.seconds(window);
		this.appName = appName;
	}
	
	public JavaStreamingContext initializeStream() {
		// create the spark configuration and spark context
		SparkConf conf = new SparkConf()
		.setAppName(this.appName);

		// create a java streaming context and define the window (2 seconds batch)
		JavaStreamingContext jssc = new JavaStreamingContext(conf, this.streamWindowSecs);

		System.out.println("Initializing Twitter stream...");
		
		return jssc;
	}
}
