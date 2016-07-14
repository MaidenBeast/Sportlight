package it.uniroma3.radeon.sa.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class StateMaker<V> {
	
	List<Tuple2<String, V>> initialEntries;
	JavaSparkContext sc;
	
	public StateMaker(JavaSparkContext sc) {
		this.initialEntries = new ArrayList<>();
		this.sc = sc;
	}
	
	public StateMaker<V> setInitialEntry(String key, V value) {
		this.initialEntries.add(new Tuple2<>(key, value));
		return this;
	}
	
	public JavaPairRDD<String, V> makeState() {
		return sc.parallelizePairs(this.initialEntries);
	}
}
