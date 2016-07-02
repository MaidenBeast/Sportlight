package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.UnlabeledTweet;
import it.uniroma3.radeon.sa.utils.TextCleaner;

import java.util.Map;

public class UnlabeledTweetMapper extends TextMapper<UnlabeledTweet> {
	
	private Map<String, String> translationRules;
	
	private static final long serialVersionUID = 1L;
	
	public UnlabeledTweetMapper(String sep, Map<String, String> translationRules) {
		super(sep);
		this.translationRules = translationRules;
	}
	
	public UnlabeledTweet call(String text) {
		String[] tweetFields = this.splitText(text);
		UnlabeledTweet tweet = new UnlabeledTweet();
		tweet.setText(new TextCleaner(this.translationRules).cleanUp(tweetFields[0]));
		return tweet;
	}
}
