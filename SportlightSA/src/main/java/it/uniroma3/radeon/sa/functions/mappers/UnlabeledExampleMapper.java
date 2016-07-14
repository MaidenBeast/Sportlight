package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.Post;
import it.uniroma3.radeon.sa.data.UnlabeledExample;
import it.uniroma3.radeon.sa.utils.TextCleaner;

import java.util.Map;

public class UnlabeledExampleMapper extends RDDMapper<String, UnlabeledExample> {
	
	private Map<String, String> translationRules;
	
	private static final long serialVersionUID = 1L;
	
	public UnlabeledExampleMapper(Map<String, String> translationRules) {
		this.translationRules = translationRules;
	}
	
	public UnlabeledExample call(String text) {
		UnlabeledExample example = new UnlabeledExample();
		example.setText(new TextCleaner(this.translationRules).cleanUp(text));
		return example;
	}
}
