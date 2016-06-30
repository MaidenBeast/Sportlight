package it.uniroma3.radeon.sa.functions.mappers;

import scala.Tuple2;
import it.uniroma3.radeon.sa.data.TweetWord;

public class WordNormalizerMapper extends PairMapper<TweetWord, String, TweetWord> {
	
	private static final long serialVersionUID = 1L;

	public TweetWord call(Tuple2<TweetWord, String> wordNorm) throws Exception {
		TweetWord rawWord = wordNorm._1();
		String normalizedWord = wordNorm._2();
		rawWord.setWord(normalizedWord);
		return rawWord;
	}

}
