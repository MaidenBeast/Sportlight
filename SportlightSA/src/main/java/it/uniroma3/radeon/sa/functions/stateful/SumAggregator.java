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

public class SumAggregator<K> extends StatefulAggregator<K, Long> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<K, Long> call(K key, Optional<Long> newValue, State<Long> prevValue)
			throws Exception {
		long sum = newValue.or(0L) + this.getStateIfExists(prevValue, 0L);
		Tuple2<K, Long> output = new Tuple2<>(key, sum);
		prevValue.update(sum);
		return output;
	}
	
	
}
