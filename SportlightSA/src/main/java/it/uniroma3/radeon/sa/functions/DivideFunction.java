package it.uniroma3.radeon.sa.functions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class DivideFunction implements VoidFunction<JavaPairRDD<String, Long>> {

	private static final long serialVersionUID = 1L;
	
	private String dividendName;
	private String divisorName;
	
	public DivideFunction() {}
	
	public DivideFunction(String dividendName, String divisorName) {
		this.dividendName = dividendName;
		this.divisorName = divisorName;
	}

	@Override
	public void call(JavaPairRDD<String, Long> factors) throws Exception {
		List<Tuple2<String,Long>> factorList = factors.collect();
		Map<String, Long> factorMap = new HashMap<>();
		for (Tuple2<String, Long> tuple : factorList) {
			factorMap.put(tuple._1(), tuple._2());
		}
		Double a = (double) factorMap.get(this.dividendName);
		Double b = (double) factorMap.get(this.divisorName);
		System.out.println(a / b);
	}
}
