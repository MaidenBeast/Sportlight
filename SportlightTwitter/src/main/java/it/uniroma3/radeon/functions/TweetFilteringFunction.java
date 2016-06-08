package it.uniroma3.radeon.functions;

import it.uniroma3.radeon.data.TweetData;
import it.uniroma3.radeon.functions.accessories.TweetCondition;

import java.lang.reflect.Method;

import org.apache.spark.api.java.function.Function;

public class TweetFilteringFunction implements Function<TweetData, Boolean>{
	
	private static final long serialVersionUID = 1L;
	private TweetCondition[] conditions;
	private TweetCondition condition;
	
	public TweetFilteringFunction(TweetCondition[] conds) {
		this.conditions = conds;
	}
	
	public TweetFilteringFunction(TweetCondition cond) {
		this.condition = cond;
	}
		
	public Boolean call(TweetData tweet) {
		return this.condition.verify(tweet);
	}
}
