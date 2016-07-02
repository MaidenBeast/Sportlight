package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.ClassificationResult;
import it.uniroma3.radeon.sa.data.UnlabeledTweet;

import org.apache.spark.mllib.classification.NaiveBayesModel;

public class ClassificationMapper extends RDDMapper<UnlabeledTweet, ClassificationResult> {
	
	private NaiveBayesModel model;
	
	public ClassificationMapper(NaiveBayesModel model) {
		this.model = model;
	}
	
	private static final long serialVersionUID = 1L;
	
	public ClassificationResult call(UnlabeledTweet unlabeled) throws Exception {
		Double predictedLabel = this.model.predict(unlabeled.getVsm());
		ClassificationResult labeled = new ClassificationResult();
		labeled.setText(unlabeled.getText());
		labeled.setSentiment(predictedLabel);
		return labeled;
	}
}
