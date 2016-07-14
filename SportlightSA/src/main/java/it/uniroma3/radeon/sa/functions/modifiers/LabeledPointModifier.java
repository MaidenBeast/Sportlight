package it.uniroma3.radeon.sa.functions.modifiers;

import it.uniroma3.radeon.sa.data.LabeledExample;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;

public class LabeledPointModifier extends Modifier<LabeledExample> {
	
	private HashingTF converter;
	private static final long serialVersionUID = 1L;
	
	public LabeledPointModifier(HashingTF converter) {
		this.converter = converter;
	}
	
	public LabeledExample call(LabeledExample raw) throws Exception {
		String text = raw.getText();
		Double sentiment = raw.getSentiment();
		List<String> words = new ArrayList<>();
		
		for (String w : text.split(" ")) {
			words.add(w);
		}
		
		LabeledPoint labeledVector = new LabeledPoint(sentiment, this.converter.transform(words));
		raw.setLabeledVector(labeledVector);
		return raw;
	}
}
