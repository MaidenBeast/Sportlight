package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.function.Function;

import it.uniroma3.radeon.sa.data.DataBean;

public class FieldExtractFunction<B extends DataBean, V> implements Function<B, V> {
	
	private String fieldName;
	
	public FieldExtractFunction(String fieldName) {
		this.fieldName = fieldName;
	}
	
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public V call(B bean) throws Exception {
		String capFieldName = Character.toUpperCase(this.fieldName.charAt(0)) + this.fieldName.substring(1);
		String getterName = "get" + capFieldName;
		try {
			Object fieldValueObj = bean.getClass().getMethod(getterName).invoke(bean);
			V value = (V) fieldValueObj;
			return value;
		}
		catch (NoSuchMethodException e) {
			e.printStackTrace();
			return null;
		}
		catch (ClassCastException e) {
			e.printStackTrace();
			return null;
		}
	}
}
