package it.uniroma3.radeon.sa.functions;

import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

public class FlattenFunction<V> implements FlatMapFunction<List<V>, V> {
	
	private static final long serialVersionUID = 1L;
	
	public Iterable<V> call(List<V> list) {
		return list;
	}
}
