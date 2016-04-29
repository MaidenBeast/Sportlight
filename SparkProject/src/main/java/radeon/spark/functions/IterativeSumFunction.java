package radeon.spark.functions;

import org.apache.spark.api.java.function.Function2;

public class IterativeSumFunction implements Function2<Integer, Integer, Integer> {
	
	private static final long serialVersionUID = 1L;

	public Integer call(Integer a, Integer b) {
		return a + b;
	}
}
