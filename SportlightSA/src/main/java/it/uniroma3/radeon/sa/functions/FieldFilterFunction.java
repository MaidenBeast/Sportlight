package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.function.Function;

import it.uniroma3.radeon.sa.utils.ObjectNavigator;

public class FieldFilterFunction<O, V> implements Function<O, Boolean> {
	
	private String fieldName;
	private V filterValue;
	private V valueIfError;
	
	public FieldFilterFunction(String fieldName, V filterValue) {
		this.fieldName = fieldName;
		this.filterValue = filterValue;
	}
	
	public FieldFilterFunction(String fieldName, V filterValue, V valueIfError) {
		this.fieldName = fieldName;
		this.filterValue = filterValue;
		this.valueIfError = valueIfError;
	}
	
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public Boolean call(O object) throws Exception {
		try {
			ObjectNavigator nav = new ObjectNavigator(object, object.getClass());
			V field = (V) nav.retrieveField(this.fieldName);
			return field.equals(this.filterValue);
		}
		catch (NullPointerException | ClassCastException e) {
			e.printStackTrace();
			return this.valueIfError.equals(this.filterValue);
		}
	}
}
