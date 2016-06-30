package it.uniroma3.radeon.sa.utils;

public class TextCleaner {
	
	public String cleanUp(String text) {
		String simplSpaces = this.simplifySpaces(text);
		String noPunct = this.removePunctuation(simplSpaces);
		String finalStr = noPunct;
		
		return finalStr;
	}
	
	public String simplifySpaces(String text) {
		String trimmed = text.trim();
		return trimmed.replaceAll(" {2,}", " ");
	}
	
	public String removePunctuation(String text) {
		return text.replaceAll("\\p{Punct}", "");
	}
}
