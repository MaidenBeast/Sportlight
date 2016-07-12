package it.uniroma3.radeon.sa.functions.stateful;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.streaming.State;

import scala.Tuple2;

import com.google.common.base.Optional;

public class ConditionalDiffAggregator<K> extends StatefulAggregator<K, Long> {
	
	private static final long serialVersionUID = 1L;
	
	private Set<K> negativeKeys;
	
	public ConditionalDiffAggregator() {
		super();
		this.negativeKeys = new HashSet<>();
	}
	
	public ConditionalDiffAggregator<K> withNegativeKey(K negativeKey) {
		this.negativeKeys.add(negativeKey);
		return this;
	}
	
	private Boolean keyIsNegative(K key) {
		return this.negativeKeys.contains(key);
	}
	
	@Override
	public Tuple2<K, Long> call(K key, Optional<Long> newValue, State<Long> prevValue)
			throws Exception {
		long sum = 0;
		if (this.keyIsNegative(key)) {
			sum = this.getStateIfExists(prevValue, 0L) - newValue.or(0L);
		}
		else {
			sum = this.getStateIfExists(prevValue, 0L) + newValue.or(0L);
		}
		Tuple2<K, Long> output = new Tuple2<>(key, sum);
		prevValue.update(sum);
		return output;
	}
}
