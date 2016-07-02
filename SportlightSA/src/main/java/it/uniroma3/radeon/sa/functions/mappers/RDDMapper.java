package it.uniroma3.radeon.sa.functions.mappers;

import org.apache.spark.api.java.function.Function;

public abstract class RDDMapper<T1, T2> implements Function<T1, T2> {

	private static final long serialVersionUID = 1L;
}
