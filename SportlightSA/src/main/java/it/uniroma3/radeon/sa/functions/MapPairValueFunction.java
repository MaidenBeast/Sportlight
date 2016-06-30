package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MapPairValueFunction<T1, T2, N> implements PairFunction<Tuple2<T1, T2>, T1, N> {
	
	private static final long serialVersionUID = 1L;
	
	private Function<T2, N> mappingFunct;
	
	public MapPairValueFunction(Function<T2, N> funct) {
		this.mappingFunct = funct;
	}
	
	public Tuple2<T1, N> call(Tuple2<T1, T2> tuple) {
		T1 el1 = tuple._1();
		T2 el2 = tuple._2();
		try {
			N newValue = this.mappingFunct.call(el2);
			return new Tuple2<>(el1, newValue);
		}
		catch (Exception e) {
			e.printStackTrace();
			return new Tuple2<>(el1, null);
		}
	}
}
