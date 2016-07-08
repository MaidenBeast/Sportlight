package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.function.Function2;

public class SumReduceFunction implements Function2<Integer, Integer, Integer> {
	
	private static final long serialVersionUID = 1L;

	public Integer call(Integer a, Integer b) {
		return a + b;
	}

}
