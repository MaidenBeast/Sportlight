package it.uniroma3.radeon.functions;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ReversePairFunction<T1, T2> implements PairFunction<Tuple2<T1, T2>, T2, T1> {
	
	private static final long serialVersionUID = 1L;

	public Tuple2<T2, T1> call(Tuple2<T1, T2> pair) {
		return new Tuple2<>(pair._2(), pair._1());
	}
}
