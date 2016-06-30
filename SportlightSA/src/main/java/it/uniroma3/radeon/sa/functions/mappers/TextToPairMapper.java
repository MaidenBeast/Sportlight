package it.uniroma3.radeon.sa.functions.mappers;

import org.apache.spark.api.java.function.PairFunction;

public abstract class TextToPairMapper<T1, T2> implements PairFunction<String, T1, T2>{
	
	private static final long serialVersionUID = 1L;
	
	private String separator;
	
	public TextToPairMapper(String sep) {
		this.separator = sep;
	}
	
	protected String[] splitText(String text) {
		return text.split(this.separator);
	}
}
