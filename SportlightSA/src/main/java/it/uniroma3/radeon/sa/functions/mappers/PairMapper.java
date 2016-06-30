package it.uniroma3.radeon.sa.functions.mappers;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public abstract class PairMapper<T1, T2, N> implements Function<Tuple2<T1, T2>, N> {

	private static final long serialVersionUID = 1L;	
}
