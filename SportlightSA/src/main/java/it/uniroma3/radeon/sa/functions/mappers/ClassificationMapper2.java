package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.ClassificationResult;
import it.uniroma3.radeon.sa.data.UnlabeledExample;

import org.apache.spark.mllib.classification.NaiveBayesModel;

public class ClassificationMapper2 extends RDDMapper<UnlabeledExample, String> {
	
	private NaiveBayesModel model;
	
	public ClassificationMapper2(NaiveBayesModel model) {
		this.model = model;
	}
	
	private static final long serialVersionUID = 1L;
	
	public String call(UnlabeledExample unlabeled) throws Exception {
		Double predictedLabel = this.model.predict(unlabeled.getVsm());
		if (predictedLabel == 0.0) {
			return "neg";
		}
		else {
			return "pos";
		}
	}
}
