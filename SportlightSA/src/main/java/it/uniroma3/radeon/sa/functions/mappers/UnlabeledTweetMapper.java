package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.UnlabeledExample;
import it.uniroma3.radeon.sa.utils.TextCleaner;

import java.util.Map;

public class UnlabeledTweetMapper extends TextMapper<UnlabeledExample> {
	
	private Map<String, String> translationRules;
	
	private static final long serialVersionUID = 1L;
	
	public UnlabeledTweetMapper(String sep, Map<String, String> translationRules) {
		super(sep);
		this.translationRules = translationRules;
	}
	
	public UnlabeledExample call(String text) {
		//Test code
//		String[] tweetFields = this.splitText(text);
//		UnlabeledTweet tweet = new UnlabeledTweet();
//		tweet.setText(new TextCleaner(this.translationRules).cleanUp(tweetFields[2]));
		
		UnlabeledExample tweet = new UnlabeledExample();
		tweet.setText(new TextCleaner(this.translationRules).cleanUp(text));
		return tweet;
	}
}
