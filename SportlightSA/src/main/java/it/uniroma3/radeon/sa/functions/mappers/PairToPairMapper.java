package it.uniroma3.radeon.sa.functions.mappers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public abstract class PairToPairMapper<A1, A2, B1, B2> implements PairFunction<Tuple2<A1, A2>, B1, B2> {

	private static final long serialVersionUID = 1L;	
}
