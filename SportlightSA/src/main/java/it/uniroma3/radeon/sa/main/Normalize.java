package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.NormalizationRule;
import it.uniroma3.radeon.sa.data.TweetTrainingExample;
import it.uniroma3.radeon.sa.data.TweetWord;
import it.uniroma3.radeon.sa.functions.FieldExtractFunction;
import it.uniroma3.radeon.sa.functions.MapPairValueFunction;
import it.uniroma3.radeon.sa.functions.PopKeyFunction;
import it.uniroma3.radeon.sa.functions.mappers.ExampleMapper;
import it.uniroma3.radeon.sa.functions.mappers.NormRuleMapper;
import it.uniroma3.radeon.sa.functions.mappers.TweetWordMapper;

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
				                                                .mapToPair(new PopKeyFunction<String, NormalizationRule>("rawText"))
				                                                .cache();
		
		//Carica la lista di parole rilevanti
		JavaRDD<String> relevantWords = sc.textFile(conf.get("RelevantWords"))
				                          .cache();
		
		//Carica i tweet da normalizzare
		JavaRDD<TweetTrainingExample> tweets = sc.textFile(conf.get("Tweets"))
				                                 .map(new ExampleMapper("\".+\""))
				                                 .cache();
		
		//Spezza i tweet in parole
		JavaPairRDD<String, TweetWord> tweetWordsMap = tweets.flatMap(new TweetWordMapper())
				                                             .mapToPair(new PopKeyFunction<String, TweetWord>("word"));
		
		//Normalizza le parole secondo le regole
	}
}
