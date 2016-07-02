package it.uniroma3.radeon.sa.data;

import java.io.Serializable;

public class TweetWord implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private Integer tweetId;
	private String word;
	
	public TweetWord() {}

	public Integer getTweetId() {
		return tweetId;
	}

	public void setTweetId(Integer tweetId) {
		this.tweetId = tweetId;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}
	
	public String toString() {
		return this.word;
	}
}
