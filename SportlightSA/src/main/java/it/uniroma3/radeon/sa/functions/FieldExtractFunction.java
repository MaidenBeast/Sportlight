package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.function.Function;

import it.uniroma3.radeon.sa.data.DataBean;
import it.uniroma3.radeon.sa.utils.ObjectNavigator;

public class FieldExtractFunction<B extends DataBean, V> implements Function<B, V> {
	
	private String fieldName;
	
	public FieldExtractFunction(String fieldName) {
		this.fieldName = fieldName;
	}
	
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public V call(B bean) throws Exception {
		try {
			ObjectNavigator nav = new ObjectNavigator(bean, bean.getClass());
			V field = (V) nav.retrieveField(this.fieldName);
			return field;
		}
		catch (NullPointerException | ClassCastException e) {
			e.printStackTrace();
			return null;
		}
	}
}
