package it.uniroma3.radeon.functions.mappers;

import it.uniroma3.radeon.data.TweetData;
import it.uniroma3.radeon.utils.TweetDataBuilder;

import org.apache.spark.api.java.function.Function;

import twitter4j.Status;

public class TweetMapper implements Function<Status, TweetData> {

	private static final long serialVersionUID = 1L;

	public TweetData call(Status stat) throws Exception {
		TweetData td = new TweetDataBuilder().buildFromStatus(stat);
		return td;
	}
	

}
