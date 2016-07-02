package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.TweetExample;
import it.uniroma3.radeon.sa.data.UnlabeledTweet;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class VectorMapper extends RDDMapper<UnlabeledTweet, UnlabeledTweet> {
	
	private HashingTF converter;
	private static final long serialVersionUID = 1L;
	
	public VectorMapper(HashingTF converter) {
		this.converter = converter;
	}
	
	public UnlabeledTweet call(UnlabeledTweet raw) {
		String text = raw.getText();
		List<String> words = new ArrayList<>();
		for (String w : text.split(" ")) {
			words.add(w);
		}
		Vector vector = this.converter.transform(words);
		raw.setVsm(vector);
		return raw;
	}

}
