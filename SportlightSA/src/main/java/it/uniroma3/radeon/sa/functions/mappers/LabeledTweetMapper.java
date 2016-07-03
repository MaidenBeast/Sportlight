package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.LabeledTweet;
import it.uniroma3.radeon.sa.utils.TextCleaner;

import java.util.Map;

public class LabeledTweetMapper extends TextMapper<LabeledTweet> {
	
	private Map<String, String> translationRules;
	
	private static final long serialVersionUID = 1L;
	
	public LabeledTweetMapper(String sep, Map<String, String> translationRules) {
		super(sep);
		this.translationRules = translationRules;
	}
	
	public LabeledTweet call(String text) {
		String[] tweetFields = this.splitText(text);
		
		Double sentiment = Double.parseDouble(tweetFields[1]);
		String rawText = tweetFields[2];
		
		LabeledTweet tweet = new LabeledTweet();
		tweet.setText(new TextCleaner(this.translationRules).cleanUp(rawText));
		tweet.setSentiment(sentiment);
		return tweet;
	}
}
