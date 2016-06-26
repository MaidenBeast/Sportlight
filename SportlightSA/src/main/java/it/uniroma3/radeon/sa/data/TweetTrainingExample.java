package it.uniroma3.radeon.sa.data;

public class TweetTrainingExample {
	
	private String normalizedText;
	private Integer sentiment;
	
	public TweetTrainingExample() {}

	public String getNormalizedText() {
		return normalizedText;
	}

	public void setNormalizedText(String normalizedText) {
		this.normalizedText = normalizedText;
	}

	public Integer getSentiment() {
		return sentiment;
	}

	public void setSentiment(Integer sentimentLabel) {
		this.sentiment = sentimentLabel;
	}
}
