package it.uniroma3.radeon.sa.functions.mappers;

import org.apache.spark.api.java.function.Function;

public abstract class JSONMapper<T> implements Function<String, T> {

	private static final long serialVersionUID = 1L;
	
	private Class<T> jsonType;
	
	public JSONMapper (Class<T> jsonType) {
		this.jsonType = jsonType;
	}
	
	protected T castToType(Object obj) {
		return this.jsonType.cast(obj);
	}
}
