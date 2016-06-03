package it.uniroma3.radeon.utils;

import java.io.Serializable;

import twitter4j.Status;
import it.uniroma3.radeon.data.TweetData;
import it.uniroma3.radeon.data.UserData;

public class TweetDataBuilder implements Serializable {
	
	private static final long serialVersionUID = 1L;

	public TweetData buildFromStatus(Status stat) {
		TweetData tweet = new TweetData();
		
		UserData tweetUser = new UserDataBuilder().buildFromUser(stat.getUser());
		tweet.setUser(tweetUser);
		tweet.setText(stat.getText());
		
		return tweet;
	}

}
