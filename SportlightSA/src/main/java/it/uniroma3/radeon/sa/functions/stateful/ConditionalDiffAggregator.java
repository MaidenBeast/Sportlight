package it.uniroma3.radeon.sa.functions.stateful;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.streaming.State;

import scala.Tuple2;

import com.google.common.base.Optional;

public class ConditionalDiffAggregator<K> extends StatefulAggregator<K, Integer> {
	
	private static final long serialVersionUID = 1L;
	
	private Map<String, K> conditions;
	
	public ConditionalDiffAggregator() {
		super();
		this.conditions = new HashMap<>();
	}
	
	public ConditionalDiffAggregator<K> withCondition(String condName, K value) {
		this.conditions.put(condName, value);
		return this;
	}
	
	private Boolean verifyCondition(String condition, K value) {
		K checkValue = this.conditions.get(condition);
		return checkValue != null && checkValue.equals(value);
	}

	@Override
	public Tuple2<K, Integer> call(K key, Optional<Integer> newValue, State<Integer> prevValue)
			throws Exception {
		int sum = newValue.or(0) + this.getStateIfExists(prevValue, 0);
		Tuple2<K, Integer> output = null;
		if (this.verifyCondition("negative", key)) {
			output = new Tuple2<>(key, (-sum));
		}
		else {
			output = new Tuple2<>(key, sum);
		}
		prevValue.update(sum);
		return output;
	}
}
