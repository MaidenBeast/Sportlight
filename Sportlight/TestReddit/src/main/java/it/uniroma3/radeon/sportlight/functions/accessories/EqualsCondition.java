package it.uniroma3.radeon.sportlight.functions.accessories;


public class EqualsCondition<T> extends Condition<T> {
	
	private static final long serialVersionUID = 1L;

	public EqualsCondition(String attribute, Object condition) {
		super(attribute, condition);
	}
	
	public Boolean verify(T data) {
		Object fieldValue = this.fieldLookup(data);
		return fieldValue.equals(this.getCondition());
	}
	

}
