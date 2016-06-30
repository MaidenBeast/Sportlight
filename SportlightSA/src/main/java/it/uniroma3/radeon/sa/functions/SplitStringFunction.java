package it.uniroma3.radeon.sa.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SplitStringFunction implements FlatMapFunction<String, String> {
	
	private String delim;
	
	public SplitStringFunction(String delim) {
		this.delim = delim;
	}
	
	private static final long serialVersionUID = 1L;
	
	public Iterable<String> call(String str) {
		String[] splits = str.split(this.delim);
		List<String> splitList = new ArrayList<>();
		
		for (String s : splits) {
			splitList.add(s);
		}
		
		return splitList;
	}
}
