package it.uniroma3.radeon.sa.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TextCleaner {
	
	private Map<String, String> translationRules;
	
	public TextCleaner(Map<String, String> translationRules) {
		this.translationRules = translationRules;
	}
	
	public String cleanUp(String text) {
		String simplSpaces = this.simplifySpaces(text);
		String translated = this.slangTranslate(simplSpaces);
		String noPunct = this.removePunctuation(translated);
		String finalStr = noPunct;
		
		return finalStr;
	}
	
	public String simplifySpaces(String text) {
		String trimmed = text.trim();
		return trimmed.replaceAll(" {2,}", " ");
	}
	
	public String slangTranslate(String text) {
		String[] words = text.split(" ");
		List<String> normWords = new ArrayList<>();
		for (String word : words) {
			String normalized;
			if ((normalized = this.translationRules.get(word)) != null) {
				normWords.add(normalized);
			}
			else {
				normWords.add(word);
			}
		}
		return this.concatList(normWords);
	}

	public String removePunctuation(String text) {
		return text.replaceAll("\\p{Punct}", "");
	}
	
	private String concatList(List<String> wordList) {
		StringBuffer sb = new StringBuffer();
		for (String word : wordList) {
			sb.append(word + " ");
		}
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}
}
