package it.uniroma3.radeon.sa.data;

import java.io.Serializable;

public class ClassificationResult implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String text;
	private Double sentiment;
	
	public ClassificationResult() {}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public Double getSentiment() {
		return sentiment;
	}

	public void setSentiment(Double sentiment) {
		this.sentiment = sentiment;
	}
	
	public String toString() {
		return "[" + this.sentiment + "]" + ", " + this.text;
	}
}
