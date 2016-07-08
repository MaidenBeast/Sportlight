package it.uniroma3.radeon.sa.functions.aggregators;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SumToMapAggregator implements VoidFunction<JavaPairRDD<String, Integer>> {
	
	private static final long serialVersionUID = 1L;
	private Accumulator<Map<String, Integer>> totalAccumulator;
	
	public SumToMapAggregator(Accumulator<Map<String, Integer>> totalAccumulator) {
		this.totalAccumulator = totalAccumulator;
	}
	
	@Override
	public void call(JavaPairRDD<String, Integer> pairRDD) throws Exception {
		final Accumulator<Map<String, Integer>> acc = this.totalAccumulator;
		pairRDD.foreach(
				new VoidFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					public void call(Tuple2<String, Integer> tuple) {
						Map<String, Integer> newCount = new HashMap<>();
						String key = tuple._1();
						Integer newValue = tuple._2();
						newCount.put(key, newValue);
						acc.merge(newCount);
					}
				}
		);
	}
}
