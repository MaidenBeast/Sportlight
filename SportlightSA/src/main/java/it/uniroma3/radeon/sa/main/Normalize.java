package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.NormalizationRule;
import it.uniroma3.radeon.sa.data.TweetTrainingExample;
import it.uniroma3.radeon.sa.functions.MakePairFunction;
import it.uniroma3.radeon.sa.functions.mappers.ExampleMapper;
import it.uniroma3.radeon.sa.functions.mappers.NormRuleMapper;

import java.io.FileReader;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.HashingTF;

public class Normalize {
	
	public static void main(String[] args) {
		String trainingFile = args[0];
		String configFile = args[1];
		
		Properties prop = new Properties();
		try {
			prop.load(new FileReader(configFile));
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("Sentiment Analysis trainer")
										.set("Tweets", trainingFile)
				                        .set("NormRules", prop.get("normRules").toString())
		                                .set("RelevantWords", prop.get("relevantWords").toString());
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Carica le regole di normalizzazione
		JavaPairRDD<String, NormalizationRule> normRulesMap = sc.textFile(conf.get("NormRules"))
				                                                .map(new NormRuleMapper("="))
				                                                .mapToPair(new MakePairFunction<String, NormalizationRule>())
				                                                .cache();
		
		//Carica la lista di parole rilevanti
		JavaRDD<String> relevantWords = sc.textFile(conf.get("RelevantWords"))
				                          .cache();
		
		//Carica i tweet da normalizzare
		JavaPairRDD<Integer, TweetTrainingExample> tweetsMap = sc.textFile(conf.get("Tweets"))
				                                                 .map(new ExampleMapper("\".+\""))
				                                                 .mapToPair(new MakePairFunction<Integer, TweetTrainingExample>())
				                                                 .sortByKey()
				                                                 .cache();
	}
}
