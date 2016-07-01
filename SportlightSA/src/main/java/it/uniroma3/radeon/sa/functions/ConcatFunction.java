package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.function.Function2;

public class ConcatFunction implements Function2<String, String, String> {
	
	private String joinString;
	
	public ConcatFunction(String joinString) {
		this.joinString = joinString;
	}
	
	public String call(String s1, String s2) {
		return s1 + this.joinString + s2;
	}

}
