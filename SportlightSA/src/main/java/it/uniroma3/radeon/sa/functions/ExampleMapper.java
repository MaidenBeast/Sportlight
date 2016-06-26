package it.uniroma3.radeon.sa.functions;

import it.uniroma3.radeon.sa.data.TweetTrainingExample;

import org.apache.spark.api.java.function.Function;

public class ExampleMapper implements Function<String, TweetTrainingExample> {

	private static final long serialVersionUID = 1L;

	public TweetTrainingExample call(String str) throws Exception {
		return null;
	}
	

}
