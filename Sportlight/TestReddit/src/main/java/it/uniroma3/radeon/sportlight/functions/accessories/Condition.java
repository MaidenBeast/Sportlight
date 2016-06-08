package it.uniroma3.radeon.sportlight.functions.accessories;

import it.uniroma3.radeon.sportlight.utils.ObjectNavigator;

import java.io.Serializable;

public abstract class Condition<T> implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String[] accessPath;
	private Object condition;
	
	public Condition(String attribute, Object condition) {
		this.accessPath = attribute.split("\\.");
		this.condition = condition;
	}
	
	protected Object fieldLookup(T data) {
		ObjectNavigator nav = new ObjectNavigator(data, data.getClass());
		return nav.retrieveField(this.accessPath);
	}
	
	public abstract Boolean verify(T data);
	
	
	public String[] getAccessPath() {
		return this.accessPath;
	}
	
	public Object getCondition() {
		return this.condition;
	}

}
