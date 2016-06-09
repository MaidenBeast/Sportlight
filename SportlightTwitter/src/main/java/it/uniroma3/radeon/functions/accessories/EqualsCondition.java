package it.uniroma3.radeon.functions.accessories;

import it.uniroma3.radeon.data.TweetData;

public class EqualsCondition extends TweetCondition {
	
	private static final long serialVersionUID = 1L;

	public EqualsCondition(String attribute, Object condition) {
		super(attribute, condition);
	}
	
	public Boolean verify(TweetData td) {
		Object fieldValue = this.fieldLookup(td);
		if (fieldValue == null) {
			System.out.println("Filtered out for null value");
			return false;
		}
		return fieldValue.equals(this.getCondition());
	}

}
