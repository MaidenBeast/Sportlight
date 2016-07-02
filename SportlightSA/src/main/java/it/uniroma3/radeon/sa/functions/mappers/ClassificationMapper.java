package it.uniroma3.radeon.sa.functions.mappers;

import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class ClassificationMapper extends RDDMapper<LabeledPoint, Tuple2<Object, Object>> {
	
	private NaiveBayesModel model;
	
	public ClassificationMapper(NaiveBayesModel model) {
		this.model = model;
	}
	
	private static final long serialVersionUID = 1L;
	
	public Tuple2<Object, Object> call(LabeledPoint point) throws Exception {
		Double trainingLabel = point.label();
		Double predictedLabel = model.predict(point.features());
		return new Tuple2<Object, Object>(predictedLabel, trainingLabel);
	}

}
