package it.uniroma3.radeon.sa.functions.aggregators;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SumToMapAggregator implements VoidFunction<JavaPairRDD<String, Integer>> {
	
	private static final long serialVersionUID = 1L;
	private Map<String, Integer> totalMap;
	
	public SumToMapAggregator(Map<String, Integer> totalMap) {
		this.totalMap = totalMap;
	}
	
	@Override
	public void call(JavaPairRDD<String, Integer> pairRDD) throws Exception {
		final Map<String, Integer> countMap = this.totalMap;
		pairRDD.foreach(
				new VoidFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					public void call(Tuple2<String, Integer> tuple) {
						String key = tuple._1();
						Integer newValue = tuple._2();
						if (countMap.containsKey(key)) {
							Integer oldValue = countMap.get(tuple._1());
							countMap.put(key, oldValue + newValue);
						}
						else {
							countMap.put(key, newValue);
						}
					}
				}
		);
	}
}
