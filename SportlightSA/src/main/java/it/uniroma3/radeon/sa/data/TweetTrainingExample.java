package it.uniroma3.radeon.sa.data;

public class TweetTrainingExample extends DataBean {
	
	private Integer id;
	private String rawText;
	private String normalizedText;
	private Integer sentiment;
	
	public TweetTrainingExample() {
		super("Id");
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
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

	public Integer getSentiment() {
		return sentiment;
	}

	public void setSentiment(Integer sentimentLabel) {
		this.sentiment = sentimentLabel;
	}
}
