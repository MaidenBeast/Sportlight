package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.Post;
import it.uniroma3.radeon.sa.data.UnlabeledTweet;
import it.uniroma3.radeon.sa.utils.TextCleaner;

import java.util.Map;

public class UnlabeledExampleMapper extends RDDMapper<Post, UnlabeledTweet> {
	
	private Map<String, String> translationRules;
	
	private static final long serialVersionUID = 1L;
	
	public UnlabeledExampleMapper(String sep, Map<String, String> translationRules) {
		this.translationRules = translationRules;
	}
	
	public UnlabeledTweet call(Post post) {
		UnlabeledTweet tweet = new UnlabeledTweet();
		String postText = post.getBody();
		tweet.setText(new TextCleaner(this.translationRules).cleanUp(postText));
		return tweet;
	}
}
