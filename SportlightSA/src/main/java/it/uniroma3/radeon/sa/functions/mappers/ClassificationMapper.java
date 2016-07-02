package it.uniroma3.radeon.sa.functions.mappers;

import it.uniroma3.radeon.sa.data.LabeledTweet;
import it.uniroma3.radeon.sa.data.UnlabeledTweet;

import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class ClassificationMapper extends RDDMapper<UnlabeledTweet, LabeledTweet> {
	
	private NaiveBayesModel model;
	
	public ClassificationMapper(NaiveBayesModel model) {
		this.model = model;
	}
	
	private static final long serialVersionUID = 1L;
	
	public LabeledTweet call(UnlabeledTweet unlabeled) throws Exception {
		Double predictedLabel = this.model.predict(unlabeled.getVsm());
		LabeledTweet labeled = new LabeledTweet();
		labeled.setText(unlabeled.getText());
		labeled.setVsm(unlabeled.getVsm());
		labeled.setSentiment(predictedLabel);
		return labeled;
	}
}
