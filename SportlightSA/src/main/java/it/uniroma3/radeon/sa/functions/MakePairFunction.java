package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MakePairFunction<O, K, V> implements PairFunction<O, K, V> {

	private static final long serialVersionUID = 1L;
	
	private String keyField;
	private String valueField;
	
	public MakePairFunction(String keyField, String valueField) {
		this.keyField = keyField;
		this.valueField = valueField;
	}

	@Override
	public Tuple2<K, V> call(O object) throws Exception {
		K key = new FieldExtractFunction<O, K>(this.keyField).call(object);
		V value = new FieldExtractFunction<O, V>(this.valueField).call(object);
		return new Tuple2<>(key, value);
	}
}
