package it.uniroma3.radeon.sa.data;

import org.apache.spark.mllib.linalg.Vector;

public class UnlabeledTweet {
	
	private String text;
	private Vector vsm;
	
	public UnlabeledTweet() {}

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
}
