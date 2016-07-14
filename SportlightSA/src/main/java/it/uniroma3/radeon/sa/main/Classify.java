package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.ClassificationResult;
import it.uniroma3.radeon.sa.data.UnlabeledExample;
import it.uniroma3.radeon.sa.functions.mappers.ClassificationMapper;
import it.uniroma3.radeon.sa.functions.mappers.UnlabeledTweetMapper;
import it.uniroma3.radeon.sa.functions.modifiers.VectorizerModifier;
import it.uniroma3.radeon.sa.utils.Parsing;

import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;

public class Classify {
	
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
		
		//Carica le regole di traduzione
		Map<String, String> translationRules = Parsing.ruleParser(prop.get("translationRules").toString(), "=");
		
		SparkConf conf = new SparkConf().setAppName("Sentiment Analysis classifier")
				                        .set("RawTweets", prop.get("rawTweets").toString())
										.set("ModelInputDir", prop.get("modelInputDir").toString())
		                                .set("ResultOutputDir", prop.get("resultOutputDir").toString());
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Crea un convertitore che traduca ogni tweet in una rappresentazione vettoriale
		HashingTF htf = new HashingTF(1000);
		
		//Carica e normalizza i tweet da classificare
		JavaRDD<UnlabeledExample> normClassSet = sc.textFile("file://" + conf.get("RawTweets"))
				                                 .map(new UnlabeledTweetMapper(",", translationRules));
		
		//Calcola una rappresentazione vettoriale dei tweet da classificare
		JavaRDD<UnlabeledExample> vsmClassSet = normClassSet.map(new VectorizerModifier(htf));
		
		//Carica il modello di classificazione
		NaiveBayesModel model = NaiveBayesModel.load(sc.sc(), "file://" + conf.get("ModelInputDir"));
		
		//Classifica i tweet per sentimento utilizzando il modello
		JavaRDD<ClassificationResult> classifiedSet = vsmClassSet.map(new ClassificationMapper(model));
		
		classifiedSet.saveAsTextFile("file://" + conf.get("ResultOutputDir"));
		sc.close();
	}
}
