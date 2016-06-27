package it.uniroma3.radeon.sa.utils;

public class TextCleaner {
	
	public String simplifySpaces(String text) {
		String trimmed = text.trim();
		return trimmed.replaceAll(" {2,}", " ");
	}
}
