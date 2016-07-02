package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.TweetExample;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;

public class LabeledPointMapper2 extends RDDMapper<TweetExample, LabeledPoint> {
	
	private HashingTF converter;
	private static final long serialVersionUID = 1L;

	public LabeledPointMapper2(HashingTF converter) {
		this.converter = converter;
	}
	
	public LabeledPoint call(TweetExample example) {
		String text = example.getRawText();
		Integer sentiment = example.getSentiment();
		
		List<String> exampleWords = new ArrayList<>();
		
		for (String word : text.split(" ")) {
			exampleWords.add(word);
		}
		
		LabeledPoint lp = new LabeledPoint(sentiment, this.converter.transform(exampleWords));
		return lp;
	}

}
