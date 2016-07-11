package it.uniroma3.radeon.sa.functions.stateful;

import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;

import scala.Tuple2;

import com.google.common.base.Optional;

public abstract class StatefulAggregator<K,V> implements Function3<K, Optional<V>, State<V>, Tuple2<K,V>>  {

	private static final long serialVersionUID = 1L;
	
	protected V getStateIfExists(State<V> state, V noStateValue) {
		if (state.exists()) {
			return (V) state.get();
		}
		else {
			return noStateValue;
		}
	}
}
