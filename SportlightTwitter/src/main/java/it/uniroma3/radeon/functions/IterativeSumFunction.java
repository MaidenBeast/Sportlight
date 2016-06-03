package it.uniroma3.radeon.functions;

import org.apache.spark.api.java.function.Function2;

public class IterativeSumFunction implements Function2<Integer, Integer, Integer> {
	
	private static final long serialVersionUID = 1L;

	public Integer call(Integer term1, Integer term2) {
		return term1 + term2;
	}

}
