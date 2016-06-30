package it.uniroma3.radeon.sa.functions.mappers;

import org.apache.spark.api.java.function.Function;

public abstract class TextMapper<T> implements Function<String, T>{
	
	private static final long serialVersionUID = 1L;
	
	private String separator;
	
	public TextMapper(String sep) {
		this.separator = sep;
	}
	
	protected String[] splitText(String text) {
		return text.split(this.separator);
	}
}
