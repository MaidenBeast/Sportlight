package it.uniroma3.radeon.functions;

public class Functions {
	
	public static MatchFunction match(Integer minLength, Integer maxLength, String pattern) {
		return new MatchFunction(minLength, maxLength, pattern);
	}
}
