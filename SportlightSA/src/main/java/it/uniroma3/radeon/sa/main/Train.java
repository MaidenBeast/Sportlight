package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.LabeledTweet;
import it.uniroma3.radeon.sa.functions.FieldExtractFunction;
import it.uniroma3.radeon.sa.functions.mappers.EvaluationMapper;
import it.uniroma3.radeon.sa.functions.mappers.LabeledTweetMapper;
import it.uniroma3.radeon.sa.functions.modifiers.LabeledPointModifier;
import it.uniroma3.radeon.sa.utils.Parsing;

import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class Train {
	
	public static void main(String[] args) {
		String configFile = args[0];
		
		Properties prop = new Properties();
		try {
			prop.load(new FileReader(configFile));
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		//Carica localmente le regole di normalizzazione
		Map<String, String> normRules = Parsing.ruleParser(prop.get("normRules").toString(), "=");
		
		SparkConf conf = new SparkConf().setAppName("Sentiment Analysis trainer")
										.set("RawTweets", prop.get("rawTweets").toString())
		                                .set("ModelOutputDir", prop.get("modelOutputDir").toString());
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Crea un convertitore che traduca ogni tweet in una rappresentazione vettoriale
		HashingTF htf = new HashingTF(1000);
		
		//Carica e normalizza il training set
		JavaRDD<LabeledTweet> normTrainingSet = sc.textFile("file://" + conf.get("RawTweets"))
				                                   .map(new LabeledTweetMapper(",", normRules));
		
		//Calcola una rappresentazione vettoriale dei tweet etichettati
		JavaRDD<LabeledTweet> vsmTrainingSet = normTrainingSet.map(new LabeledPointModifier(htf));
		
		//Dividi il training set in training e test
		JavaRDD<LabeledPoint>[] splitSet = vsmTrainingSet.map(new FieldExtractFunction<LabeledTweet, LabeledPoint>("labeledVector"))
				                                         .randomSplit(new double[]{0.6, 0.4}, 11L);
		JavaRDD<LabeledPoint> training = splitSet[0];
		JavaRDD<LabeledPoint> test = splitSet[1];
		
		//Calcola il modello di classificazione
		NaiveBayesModel model = NaiveBayes.train(training.rdd());
		
		//Effettua la classificazione sul test set
		JavaRDD<Tuple2<Object, Object>> classResults = test.map(new EvaluationMapper(model));
		
		//Stampa a video una metrica di valutazione del modello (F-Measure)
		MulticlassMetrics stats = new MulticlassMetrics(classResults.rdd());
		for (double label : stats.labels()) {
			System.out.println(label + " : " + stats.fMeasure(label));
		}
		
		//Salva il modello per poterlo applicare in fase di classificazione pura
		model.save(sc.sc(), "file://" + conf.get("ModelOutputDir"));
		sc.close();
	}
}
