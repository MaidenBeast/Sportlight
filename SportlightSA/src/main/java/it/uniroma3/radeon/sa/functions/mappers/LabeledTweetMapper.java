package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.LabeledExample;
import it.uniroma3.radeon.sa.utils.TextCleaner;

import java.util.Map;

public class LabeledTweetMapper extends TextMapper<LabeledExample> {
	
	private Map<String, String> translationRules;
	
	private static final long serialVersionUID = 1L;
	
	public LabeledTweetMapper(String sep, Map<String, String> translationRules) {
		super(sep);
		this.translationRules = translationRules;
	}
	
	public LabeledExample call(String text) {
		String[] tweetFields = this.splitText(text);
		
		Double sentiment = Double.parseDouble(tweetFields[1]);
		String rawText = tweetFields[2];
		
		LabeledExample tweet = new LabeledExample();
		tweet.setText(new TextCleaner(this.translationRules).cleanUp(rawText));
		tweet.setSentiment(sentiment);
		return tweet;
	}
}
