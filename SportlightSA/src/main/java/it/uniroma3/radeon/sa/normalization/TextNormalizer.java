package it.uniroma3.radeon.sa.normalization;

import it.uniroma3.radeon.sa.utils.Parsing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class TextNormalizer {
	
	private Map<String, String> raw2norm;
	private List<String> relevantWords;
	
	public TextNormalizer(String normFile, String relFile) {
		this.raw2norm = Parsing.parseToMap(new TreeMap<String, String>(), normFile, "=");
		this.relevantWords = Parsing.parseToList(new ArrayList<String>(), relFile);
	}
	
	public String normalizeText(String rawText) {
		StringTokenizer st = new StringTokenizer(rawText.toLowerCase());
		StringBuffer normalized = new StringBuffer();
		while (st.hasMoreTokens()) {
			String rawWord = st.nextToken();
			if (this.raw2norm.containsKey(rawWord)) {
				normalized.append(this.raw2norm.get(rawWord) + " ");
			}
			else {
				normalized.append(rawWord + " ");
			}
		}
		return this.endProcessing(normalized);
	}
	
	public String retainRelevantWords(String normText) {
		StringTokenizer st = new StringTokenizer(normText);
		StringBuffer clean = new StringBuffer();
		while (st.hasMoreTokens()) {
			String word = st.nextToken();
			if (this.relevantWords.contains(word)) {
				clean.append(word + " ");
			}
		}
		return this.endProcessing(clean);
	}
	
	private String endProcessing(StringBuffer buffer) {
		buffer.deleteCharAt(buffer.length() - 1);
		return buffer.toString();
	}

}
