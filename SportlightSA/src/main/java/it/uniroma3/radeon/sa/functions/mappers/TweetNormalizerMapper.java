package it.uniroma3.radeon.sa.functions.mappers;

import scala.Tuple2;
import it.uniroma3.radeon.sa.data.TweetTrainingExample;
import it.uniroma3.radeon.sa.data.TweetWord;

public class TweetNormalizerMapper extends PairMapper<TweetTrainingExample, Iterable<TweetWord>, TweetTrainingExample> {
	
	private static final long serialVersionUID = 1L;

	public TweetTrainingExample call(Tuple2<TweetTrainingExample, Iterable<TweetWord>> tuple) {
		TweetTrainingExample original = tuple._1();
		Iterable<TweetWord> normalizedWords = tuple._2();
		original.setRawText(this.joinWords(normalizedWords));
		return original;
	}
	
	private String joinWords(Iterable<TweetWord> words) {
		StringBuffer sb = new StringBuffer();
		for (TweetWord tw : words) {
			sb.append(tw.getWord() + " ");
		}
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

}
