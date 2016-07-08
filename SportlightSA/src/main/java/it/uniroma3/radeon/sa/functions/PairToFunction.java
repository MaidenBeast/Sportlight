package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PairToFunction<O, V> implements PairFunction<O, O, V> {
	
	private V value;
	
	public PairToFunction(V value) {
		this.value = value;
	}
	
	private static final long serialVersionUID = 1L;
	
	public Tuple2<O, V> call(O object) {
		return new Tuple2<>(object, this.value);
	}
}
