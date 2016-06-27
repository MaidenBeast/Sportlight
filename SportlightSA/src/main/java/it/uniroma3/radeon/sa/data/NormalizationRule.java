package it.uniroma3.radeon.sa.data;

public class NormalizationRule extends DataBean {
	
	private String rawText;
	private String normalizedText;
	
	public NormalizationRule() {
		super("RawText");
	}

	public String getRawText() {
		return rawText;
	}

	public void setRawText(String rawText) {
		this.rawText = rawText;
	}

	public String getNormalizedText() {
		return normalizedText;
	}

	public void setNormalizedText(String normalizedText) {
		this.normalizedText = normalizedText;
	}
}
