package it.uniroma3.radeon.sa.functions.mappers;

import java.util.ArrayList;
import java.util.List;

import it.uniroma3.radeon.sa.data.stateful.StateEntry;

import org.apache.spark.api.java.function.FlatMapFunction;

public abstract class StateDefinerMapper<T> implements FlatMapFunction<T, StateEntry<T>> {
	
	private static final long serialVersionUID = 1L;
}
