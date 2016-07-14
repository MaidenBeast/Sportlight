package it.uniroma3.radeon.sa.data;

import java.io.Serializable;

import org.apache.spark.mllib.regression.LabeledPoint;

public class LabeledExample implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String text;
	private LabeledPoint labeledVector;
	private Double sentiment;
	
	public LabeledExample() {}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
	
	public LabeledPoint getLabeledVector() {
		return this.labeledVector;
	}
	
	public void setLabeledVector(LabeledPoint lp) {
		this.labeledVector = lp;
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
