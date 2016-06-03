package it.uniroma3.radeon.functions;

import it.uniroma3.radeon.data.TweetData;
import it.uniroma3.radeon.functions.accessories.TweetCondition;

import java.lang.reflect.Method;

import org.apache.spark.api.java.function.Function;

public class TweetFilteringFunction implements Function<TweetData, Boolean>{
	
	private static final long serialVersionUID = 1L;
	private TweetCondition[] conditions;
	private String feature;
	private String attribute;
	private String condition;
	
	public TweetFilteringFunction(TweetCondition[] conds) {
		this.conditions = conds;
	}
	
	public TweetFilteringFunction(String featAttr, String cond) {
		String[] parameters = featAttr.split("\\.");
		this.feature = parameters[0];
		this.attribute = parameters[1];
		this.condition = cond;
	}
	
	public Boolean call(TweetData tweet) {
		for (TweetCondition condition : this.conditions) {
			if (!condition.verify(tweet)) {
				return false;
			}
		}
		return true;
	}
}
