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
		String lowerCase = this.uncapitalize(noPunct);
		String finalStr = lowerCase;
		
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
	
	public String uncapitalize(String text) {
		String[] words = this.splitText(text);
		List<String> lowerCaseWords = new ArrayList<>();
		for (String word : words) {
			lowerCaseWords.add(word.toLowerCase());
		}
		return this.concatList(lowerCaseWords);
	}
	
	private String[] splitText(String text) {
		return text.split(" ");
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
