package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class GetPairValueFunction<K, V> implements Function<Tuple2<K, V>, V> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public V call(Tuple2<K, V> tuple) throws Exception {
		return tuple._2();
	}
}
