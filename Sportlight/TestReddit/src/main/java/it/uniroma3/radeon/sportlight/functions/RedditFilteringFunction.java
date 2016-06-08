package it.uniroma3.radeon.sportlight.functions;

import it.uniroma3.radeon.sportlight.data.RedditPostData;
import it.uniroma3.radeon.sportlight.functions.accessories.Condition;

import org.apache.spark.api.java.function.Function;

public class RedditFilteringFunction implements Function<RedditPostData, Boolean> {

	private static final long serialVersionUID = 1L;
	private Condition<RedditPostData> condition;
	
	public RedditFilteringFunction(Condition<RedditPostData> cond) {
		this.condition = cond;
	}

	public Boolean call(RedditPostData data) throws Exception {
		return this.condition.verify(data);
	}
}
