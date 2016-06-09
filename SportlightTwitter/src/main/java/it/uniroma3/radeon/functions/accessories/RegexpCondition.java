package it.uniroma3.radeon.functions.accessories;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.uniroma3.radeon.data.TweetData;

public class RegexpCondition extends TweetCondition {
	
	private Pattern pattern;
	
	private static final long serialVersionUID = 1L;
	
	public RegexpCondition(String attribute, String regexp) {
		super(attribute);
		this.pattern = this.compileExpr(regexp);
	}
	
	public Boolean verify(TweetData td) {
		Object fieldValue = this.fieldLookup(td);
		String valueString = fieldValue.toString();
		
		Matcher m = this.pattern.matcher(valueString);
		return m.matches();
	}
	
	private Pattern compileExpr(String regexp) {
		return Pattern.compile(regexp);
	}

}
