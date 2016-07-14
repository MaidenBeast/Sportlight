package it.uniroma3.radeon.sa.functions;

import java.util.Collection;

import org.apache.spark.api.java.function.Function;

import it.uniroma3.radeon.sa.utils.ObjectNavigator;

public class FieldContainsFunction<O, V> implements Function<O, Boolean> {
	
	private String fieldName;
	private V valueToSearch;
	
	public FieldContainsFunction(String fieldName, V valToSearch) {
		this.fieldName = fieldName;
		this.valueToSearch = valToSearch;
	}
		
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public Boolean call(O object) throws Exception {
		try {
			ObjectNavigator nav = new ObjectNavigator(object, object.getClass());
			Collection<V> field = (Collection<V>) nav.retrieveField(this.fieldName);
			return field.contains(this.valueToSearch);
		}
		catch (NullPointerException | ClassCastException e) {
			e.printStackTrace();
			return false;
		}
	}
}
