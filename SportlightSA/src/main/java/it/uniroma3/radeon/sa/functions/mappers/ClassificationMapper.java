package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.ClassificationResult;
import it.uniroma3.radeon.sa.data.UnlabeledExample;

import org.apache.spark.mllib.classification.NaiveBayesModel;

public class ClassificationMapper extends RDDMapper<UnlabeledExample, ClassificationResult> {
	
	private NaiveBayesModel model;
	
	public ClassificationMapper(NaiveBayesModel model) {
		this.model = model;
	}
	
	private static final long serialVersionUID = 1L;
	
	public ClassificationResult call(UnlabeledExample unlabeled) throws Exception {
		Double predictedLabel = this.model.predict(unlabeled.getVsm());
		ClassificationResult labeled = new ClassificationResult();
		labeled.setText(unlabeled.getText());
		if (predictedLabel == 0.0) {
			labeled.setSentiment("neg");
		}
		else {
			labeled.setSentiment("pos");
		}
		return labeled;
	}
}
