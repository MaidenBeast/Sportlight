package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.NormalizationRule;

public class NormRuleMapper extends TextMapper<NormalizationRule> {

	private static final long serialVersionUID = 1L;
	
	public NormRuleMapper(String sep) {
		super(sep);
	}

	public NormalizationRule call(String text) throws Exception {
		String[] rawNorm = this.splitText(text);
		NormalizationRule rule = new NormalizationRule();
		rule.setRawText(rawNorm[0]);
		rule.setNormalizedText(rawNorm[1]);
		return rule;
	}
}
