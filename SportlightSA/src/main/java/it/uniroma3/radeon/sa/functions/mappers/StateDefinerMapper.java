package it.uniroma3.radeon.sa.functions.mappers;

import org.apache.spark.api.java.function.PairFlatMapFunction;

public abstract class StateDefinerMapper<T> implements PairFlatMapFunction<T, String, T> {
	
	private static final long serialVersionUID = 1L;
}
