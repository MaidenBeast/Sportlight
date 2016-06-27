package it.uniroma3.radeon.sa.functions.mappers;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;

public abstract class TextMapper<T> implements Function<String, T>{
	
	private static final long serialVersionUID = 1L;
	
	private String separator;
	private String pattern;
	
	public TextMapper(String sep) {
		this.separator = sep;
	}
	
	public TextMapper(String sepString, String kind) {
		if (kind.equals("pattern")) {
			this.pattern = sepString;
		}
		else {
			this.separator = sepString;
		}
	}
	
	protected String[] splitText(String text) {
		return text.split(this.separator);
	}
	
	protected String[] matchPattern(String text) {
		Matcher matcher = Pattern.compile(this.pattern).matcher(text);
		List<String> fieldsMatched = new ArrayList<>();
		while (matcher.find()) {
			String currentMatch = matcher.group();
			fieldsMatched.add(currentMatch.substring(1, currentMatch.length()-1));
		}
		return fieldsMatched.toArray(new String[0]);
	}
}
