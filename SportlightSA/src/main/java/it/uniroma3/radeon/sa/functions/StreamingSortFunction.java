package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

public class StreamingSortFunction<K, V> implements Function<JavaPairRDD<K, V>, JavaPairRDD<K, V>> {

	private static final long serialVersionUID = 1L;

	@Override
	public JavaPairRDD<K, V> call(JavaPairRDD<K, V> unsorted) throws Exception {
		return unsorted.sortByKey(false);
	}
}
