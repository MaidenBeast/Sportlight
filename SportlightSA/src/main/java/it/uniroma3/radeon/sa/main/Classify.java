package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.TweetExample;
import it.uniroma3.radeon.sa.functions.FieldExtractFunction;
import it.uniroma3.radeon.sa.functions.PopKeyFunction;
import it.uniroma3.radeon.sa.functions.mappers.ExampleMapper;
import it.uniroma3.radeon.sa.functions.mappers.LocalVectorMapper;
import it.uniroma3.radeon.sa.utils.Parsing;

import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;

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
				                        .set("Tweets", prop.get("tweets").toString())
										.set("Model", prop.get("modelDir").toString())
		                                .set("ResultOutput", prop.get("output").toString());
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Crea un convertitore che traduca ogni tweet in una rappresentazione vettoriale
		HashingTF htf = new HashingTF(1000);
		
		//Carica e normalizza i tweet da classificare
		JavaPairRDD<Integer, TweetExample> normClassSet = sc.textFile("file://" + conf.get("Tweets"))
				                                            .map(new ExampleMapper(",", translationRules))
				                                            .mapToPair(new PopKeyFunction<Integer, TweetExample>("id"))
				                                            .cache();
		
		//Carica il modello di classificazione
		NaiveBayesModel model = NaiveBayesModel.load(sc.sc(), "file://" + conf.get("Model"));
		
		//Usa il modello per classificare il classSet
		sc.close();
	}
}
