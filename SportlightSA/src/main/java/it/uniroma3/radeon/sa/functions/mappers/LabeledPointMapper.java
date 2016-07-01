package it.uniroma3.radeon.sa.functions.mappers;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;

public class LabeledPointMapper extends TextMapper<LabeledPoint> {
	
	private HashingTF converter;
	private static final long serialVersionUID = 1L;

	public LabeledPointMapper(String sep, HashingTF converter) {
		super(sep);
		this.converter = converter;
	}
	
	public LabeledPoint call(String example) {
		String[] exampleFields = this.splitText(example);
		Integer sentiment = Integer.parseInt(exampleFields[1]);
		String text = exampleFields[2];
		
		List<String> exampleWords = new ArrayList<>();
		
		for (String word : text.split(" ")) {
			exampleWords.add(word);
		}
		
		LabeledPoint lp = new LabeledPoint(sentiment, this.converter.transform(exampleWords));
		return lp;
	}

}
