package it.uniroma3.radeon.data;

import java.io.Serializable;

public class TweetData implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private UserData user;
	private String text;
	private Boolean isRetweet;
	
	public TweetData() {}
	
	public TweetData(UserData user, String text) {
		this.user = user;
		this.text = text;
	}

	public UserData getUser() {
		return user;
	}

	public void setUser(UserData user) {
		this.user = user;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
	
	public Boolean getIsRetweet() {
		return isRetweet;
	}

	public void setIsRetweet(Boolean isRetweet) {
		this.isRetweet = isRetweet;
	}

	public String toString() {
		return "User: " + this.user.getName() + "\n" +
	           this.getText() + " [" + this.getIsRetweet() + "]";
	}
}
