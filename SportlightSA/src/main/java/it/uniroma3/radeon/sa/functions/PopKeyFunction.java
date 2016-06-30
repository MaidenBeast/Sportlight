package it.uniroma3.radeon.sa.functions;

import it.uniroma3.radeon.sa.data.DataBean;
import it.uniroma3.radeon.sa.utils.ObjectNavigator;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PopKeyFunction<K, V> implements PairFunction<V, K, V> {
	
	private String keyField;
	
	public PopKeyFunction(String keyField) {
		this.keyField = keyField;
	}
	
	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unchecked")
	public Tuple2<K, V> call(V value) throws Exception {
		try {
			ObjectNavigator nav = new ObjectNavigator(value, value.getClass());
			K key = (K) nav.retrieveField(this.keyField);
			return new Tuple2<>(key, value);
		}
		catch (NullPointerException | ClassCastException e) {
			e.printStackTrace();
			return new Tuple2<>(null, value);
		}
	}
}
