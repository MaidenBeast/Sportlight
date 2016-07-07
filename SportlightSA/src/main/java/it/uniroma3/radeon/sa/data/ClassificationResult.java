package it.uniroma3.radeon.sa.data;

import java.io.Serializable;

public class ClassificationResult implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String text;
	private String sentiment;
	
	public ClassificationResult() {}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getSentiment() {
		return sentiment;
	}

	public void setSentiment(String sentiment) {
		this.sentiment = sentiment;
	}
	
	public String toString() {
		return "[" + this.sentiment + "]" + ", " + this.text;
	}
}
