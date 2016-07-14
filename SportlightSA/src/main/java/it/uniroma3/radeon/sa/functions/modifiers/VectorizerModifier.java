package it.uniroma3.radeon.sa.functions.modifiers;

import it.uniroma3.radeon.sa.data.UnlabeledExample;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;

public class VectorizerModifier extends Modifier<UnlabeledExample> {
	
	private HashingTF converter;
	private static final long serialVersionUID = 1L;
	
	public VectorizerModifier(HashingTF converter) {
		this.converter = converter;
	}

	public UnlabeledExample call(UnlabeledExample raw) throws Exception {
		String text = raw.getText();
		List<String> words = new ArrayList<>();
		for (String w : text.split(" ")) {
			words.add(w);
		}
		Vector vector = this.converter.transform(words);
		raw.setVsm(vector);
		return raw;
	}
}
