package it.uniroma3.radeon.functions.accessories;

import java.io.Serializable;

import it.uniroma3.radeon.data.TweetData;

public abstract class TweetCondition implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String[] accessList;
	private Object condition;
	
	public TweetCondition(String attribute, Object condition) {
		this.accessList = attribute.split("\\.");
		this.condition = condition;
	}
	
	public abstract Boolean verify(TweetData td);
	
	public String[] getAccessList() {
		return this.accessList;
	}
	
	public Object getCondition() {
		return this.condition;
	}

}
