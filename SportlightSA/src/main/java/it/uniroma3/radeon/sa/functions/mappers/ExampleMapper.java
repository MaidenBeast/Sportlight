package it.uniroma3.radeon.sa.functions.mappers;

import java.util.Map;

import it.uniroma3.radeon.sa.data.TweetExample;
import it.uniroma3.radeon.sa.utils.TextCleaner;

public class ExampleMapper extends TextMapper<TweetExample> {
	
	private Map<String, String> translationRules;
	
	private static final long serialVersionUID = 1L;
	
	public ExampleMapper(String sep, Map<String, String> translationRules) {
		super(sep);
		this.translationRules = translationRules;
	}
	
	public TweetExample call(String text) throws Exception {
		String[] tweetFields = this.splitText(text);
		TweetExample example = new TweetExample();
		example.setId(Integer.parseInt(tweetFields[0]));
		example.setSentiment(Integer.parseInt(tweetFields[1]));
		example.setRawText(new TextCleaner(this.translationRules).cleanUp(tweetFields[2]));
		return example;
	}
}
