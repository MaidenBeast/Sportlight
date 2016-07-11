package it.uniroma3.radeon.sa.functions;

import org.apache.spark.api.java.function.Function2;

public class SumReduceFunction implements Function2<Long, Long, Long> {
	
	private static final long serialVersionUID = 1L;

	public Long call(Long a, Long b) {
		return a + b;
	}

}
