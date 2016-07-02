package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.function.Function;

import it.uniroma3.radeon.sa.utils.ObjectNavigator;

public class FieldExtractFunction<O, V> implements Function<O, V> {
	
	private String fieldName;
	
	public FieldExtractFunction(String fieldName) {
		this.fieldName = fieldName;
	}
	
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public V call(O object) throws Exception {
		try {
			ObjectNavigator nav = new ObjectNavigator(object, object.getClass());
			V field = (V) nav.retrieveField(this.fieldName);
			return field;
		}
		catch (NullPointerException | ClassCastException e) {
			e.printStackTrace();
			return null;
		}
	}
}
