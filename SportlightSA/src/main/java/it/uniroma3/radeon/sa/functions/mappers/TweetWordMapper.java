package it.uniroma3.radeon.sa.functions.mappers;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import it.uniroma3.radeon.sa.data.TweetTrainingExample;
import it.uniroma3.radeon.sa.data.TweetWord;
import it.uniroma3.radeon.sa.utils.TextCleaner;

public class TweetWordMapper implements FlatMapFunction<TweetTrainingExample, TweetWord> {

	private static final long serialVersionUID = 1L;
	
	public Iterable<TweetWord> call(TweetTrainingExample example) throws Exception {
		String rawText = example.getRawText();
		String[] words = rawText.split(" ");
		List<TweetWord> wordList = new ArrayList<>();
		for (String word : words) {
			TweetWord tw = new TweetWord();
			tw.setTweetId(example.getId());
			tw.setWord(word);
			wordList.add(tw);
		}
		return wordList;
	}
}
