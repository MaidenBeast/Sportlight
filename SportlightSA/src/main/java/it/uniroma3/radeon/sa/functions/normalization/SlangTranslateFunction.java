package it.uniroma3.radeon.sa.functions.normalization;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;

import it.uniroma3.radeon.sa.data.TweetExample;

public class SlangTranslateFunction implements Function<TweetExample, TweetExample> {
	
	private Map<String, String> translationRules;
	
	private static final long serialVersionUID = 1L;
	
	public SlangTranslateFunction(Map<String, String> translationRules) {
		this.translationRules = translationRules;
	}

	public TweetExample call(TweetExample example) {
		String rawText = example.getRawText();
		String[] words = rawText.split(" ");
		List<String> normWords = new ArrayList<>();
		for (String w : words) {
			String normalized;
			if ((normalized = this.translationRules.get(w)) != null) {
				normWords.add(normalized);
			}
			else {
				normWords.add(w);
			}
		}
		example.setRawText(this.concatList(normWords));
		return example;
	}
	
	private String concatList(List<String> list) {
		StringBuffer sb = new StringBuffer();
		for (String word : list) {
			sb.append(word + " ");
		}
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

}
