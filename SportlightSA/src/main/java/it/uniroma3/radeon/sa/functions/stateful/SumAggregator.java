package it.uniroma3.radeon.sa.functions.stateful;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;

import com.google.common.base.Optional;

import scala.Tuple2;

public class SumAggregator<K> extends StatefulAggregator<K, Integer> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<K, Integer> call(K key, Optional<Integer> newValue, State<Integer> prevValue)
			throws Exception {
		int sum = newValue.or(0) + this.getStateIfExists(prevValue, 0);
		Tuple2<K, Integer> output = new Tuple2<>(key, sum);
		prevValue.update(sum);
		return output;
	}
	
	
}
