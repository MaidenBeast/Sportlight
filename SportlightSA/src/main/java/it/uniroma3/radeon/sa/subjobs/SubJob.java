package it.uniroma3.radeon.sa.subjobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class SubJob {
	
	private JavaSparkContext context;
	private SparkConf config;
	
	public SubJob(JavaSparkContext context, SparkConf config) {
		this.context = context;
		this.config = config;
	}
	
	public abstract JavaRDD<?> execute();
	
	public JavaSparkContext getContext() {
		return this.context;
	}
	
	public SparkConf getConfig() {
		return this.config;
	}
}
