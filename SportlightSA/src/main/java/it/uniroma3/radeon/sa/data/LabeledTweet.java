package it.uniroma3.radeon.sa.data;

import org.apache.spark.mllib.linalg.Vector;

public class LabeledTweet {
	
	private String text;
	private Vector vsm;
	private Double sentiment;
	
	public LabeledTweet() {}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public Vector getVsm() {
		return vsm;
	}

	public void setVsm(Vector vsm) {
		this.vsm = vsm;
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
