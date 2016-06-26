package it.uniroma3.radeon.sa.utils;

import it.uniroma3.radeon.sa.data.TweetTrainingExample;

public class ExampleBuilder {
	
	public TweetTrainingExample buildExample(String rawText, Integer label) {
		TweetTrainingExample example = new TweetTrainingExample();
		//example.setNormalizedText(normalizer.normalize(rawText));
		example.setSentiment(label);
		return example;
	}
	
	public TweetTrainingExample buildExampleFromText(String example, String separator) {
		String[] textLabel = example.split(separator);
		return null;
	}

}
