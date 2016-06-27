package it.uniroma3.radeon.sa.functions;

import it.uniroma3.radeon.sa.data.DataBean;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MakePairFunction<K, V extends DataBean> implements PairFunction<V, K, V> {
	
	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unchecked")
	public Tuple2<K, V> call(V value) throws Exception {
		String keyGetterName = "get" + value.getKeyField();
		try {
			Object keyObj = value.getClass().getMethod(keyGetterName).invoke(value);
			K key = (K) keyObj;
			return new Tuple2<>(key, value);
		}
		catch (NoSuchMethodException e) {
			e.printStackTrace();
			return new Tuple2<>(null, value);
		}
		catch (ClassCastException e) {
			e.printStackTrace();
			return new Tuple2<>(null, value);
		}
	}
}
