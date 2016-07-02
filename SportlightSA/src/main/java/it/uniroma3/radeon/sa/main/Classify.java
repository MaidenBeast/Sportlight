package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.TweetExample;
import it.uniroma3.radeon.sa.functions.FieldExtractFunction;
import it.uniroma3.radeon.sa.functions.mappers.ClassificationMapper;
import it.uniroma3.radeon.sa.functions.mappers.LabeledPointMapper;
import it.uniroma3.radeon.sa.functions.mappers.LocalVectorMapper;
import it.uniroma3.radeon.sa.subjobs.NormalizationJob;

import java.io.FileReader;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class Classify {
	
	@SuppressWarnings("unchecked")
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
		
		SparkConf conf = new SparkConf().setAppName("Sentiment Analysis classifier")
				                        .set("Tweets", prop.get("tweets").toString())
										.set("Model", prop.get("modelDir").toString())
		                                .set("ResultOutput", prop.get("output").toString());
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Crea un convertitore che traduca ogni tweet in una rappresentazione vettoriale
		HashingTF htf = new HashingTF(1000);
		
		//Normalizza il set di tweet
		JavaRDD<TweetExample> normalizedTweets = (JavaRDD<TweetExample>) new NormalizationJob(sc, conf).execute();
		
		//Ottieni i vettori per la classificazione dal set normalizzato
		JavaRDD<Vector> classSet = normalizedTweets.map(new FieldExtractFunction<TweetExample, String>("rawText"))
				                                   .map(new LocalVectorMapper(htf));
		
		//Carica il modello di classificazione
		NaiveBayesModel model = NaiveBayesModel.load(sc.sc(), "file://" + conf.get("Model"));
		
		//Usa il modello per classificare il classSet

		sc.close();
	}
}
