package it.uniroma3.radeon.sa.functions.mappers;

import scala.Tuple2;
import it.uniroma3.radeon.sa.data.NormalizationRule;

public class NormRulePairMapper extends TextToPairMapper<String, String> {

	private static final long serialVersionUID = 1L;
	
	public NormRulePairMapper(String sep) {
		super(sep);
	}

	public Tuple2<String, String> call(String text) throws Exception {
		String[] rawNorm = this.splitText(text);
		return new Tuple2<>(rawNorm[0], rawNorm[1]);
	}
}
