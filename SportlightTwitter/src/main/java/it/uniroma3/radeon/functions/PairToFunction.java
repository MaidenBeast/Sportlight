package it.uniroma3.radeon.functions;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PairToFunction<T1, T2> implements PairFunction<T1, T1, T2> {
	
	private T2 pairElement;
	
	public PairToFunction(T2 pairElement) {
		this.pairElement = pairElement;
	}

	private static final long serialVersionUID = 1L;

	public Tuple2<T1, T2> call(T1 element) {
		return new Tuple2<>(element, this.pairElement);
	}
}
