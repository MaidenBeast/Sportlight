package it.uniroma3.radeon.sportlight.data;

import java.io.Serializable;

public class RedditPostData implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String title;
	private String subreddit;
	private String text;
	
	public RedditPostData() {}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getSubreddit() {
		return subreddit;
	}

	public void setSubreddit(String subreddit) {
		this.subreddit = subreddit;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
}
