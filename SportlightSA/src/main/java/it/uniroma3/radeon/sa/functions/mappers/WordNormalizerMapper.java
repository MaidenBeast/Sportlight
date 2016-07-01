package it.uniroma3.radeon.sa.functions.mappers;

import com.google.common.base.Optional;

import scala.Tuple2;
import it.uniroma3.radeon.sa.data.TweetWord;

public class WordNormalizerMapper extends PairMapper<TweetWord, Optional<String>, TweetWord> {
	
	private static final long serialVersionUID = 1L;

	public TweetWord call(Tuple2<TweetWord, Optional<String>> wordNorm) throws Exception {
		TweetWord rawWord = wordNorm._1();
		Optional<String> normalizedWord = wordNorm._2();
		if (normalizedWord.isPresent()) {
			rawWord.setWord(normalizedWord.get());
		}
		return rawWord;
	}

}
