package it.uniroma3.radeon.sa.data;

import java.io.Serializable;

import org.apache.spark.mllib.linalg.Vector;

public class UnlabeledExample implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String text;
	private Vector vsm;
	
	public UnlabeledExample() {}

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
	
	public String toString() {
		return this.text;
	}
}
