package it.uniroma3.radeon.sa.functions.mappers;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

public class LocalVectorMapper extends RDDMapper<String, Vector> {
	
	private HashingTF converter;
	private static final long serialVersionUID = 1L;
	
	public LocalVectorMapper(HashingTF converter) {
		this.converter = converter;
	}
	
	public Vector call(String text) {
		List<String> words = new ArrayList<>();
		for (String w : text.split(" ")) {
			words.add(w);
		}
		Vector vector = this.converter.transform(words);
		return vector;
	}

}
