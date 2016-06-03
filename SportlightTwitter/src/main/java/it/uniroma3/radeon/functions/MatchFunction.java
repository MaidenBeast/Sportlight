package it.uniroma3.radeon.functions;

import org.apache.spark.api.java.function.Function;

public class MatchFunction implements Function<String, Boolean>{
	
	private static final long serialVersionUID = 1L;
	private Integer minLength;
	private Integer maxLength;
	private String pattern;
	
	public MatchFunction(Integer minLength, Integer maxLength, String pattern) {
		this.minLength = minLength;
		this.maxLength = maxLength;
		this.pattern = pattern;
	}
	
	public Boolean call(String str) {
		return str.matches(this.pattern)
				&& (this.minLength == 0 || str.length() >= this.minLength)
				&& (this.maxLength == 0 || str.length() <= this.maxLength);
	}
}
