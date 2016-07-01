package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.TweetTrainingExample;
import it.uniroma3.radeon.sa.utils.TextCleaner;

public class ExampleMapper extends TextMapper<TweetTrainingExample> {

	private static final long serialVersionUID = 1L;
	
	public ExampleMapper(String sep) {
		super(sep);
	}
	
	public TweetTrainingExample call(String text) throws Exception {
		String[] tweetFields = this.splitText(text);
		TweetTrainingExample example = new TweetTrainingExample();
		example.setId(Integer.parseInt(tweetFields[0]));
		example.setSentiment(Integer.parseInt(tweetFields[1]));
		example.setRawText(new TextCleaner().simplifySpaces(tweetFields[2]));
		return example;
	}
}
