package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.TweetTrainingExample;
import it.uniroma3.radeon.sa.data.TweetWord;
import it.uniroma3.radeon.sa.functions.GetPairValueFunction;
import it.uniroma3.radeon.sa.functions.PopKeyFunction;
import it.uniroma3.radeon.sa.functions.mappers.ExampleMapper;
import it.uniroma3.radeon.sa.functions.mappers.NormRulePairMapper;
import it.uniroma3.radeon.sa.functions.mappers.TweetNormalizerMapper;
import it.uniroma3.radeon.sa.functions.mappers.TweetWordMapper;
import it.uniroma3.radeon.sa.functions.mappers.WordNormalizerMapper;

import java.io.FileReader;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Normalize {
	
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
		
		SparkConf conf = new SparkConf().setAppName("Sentiment Analysis trainer")
										.set("Tweets", prop.get("tweets").toString())
				                        .set("NormRules", prop.get("normRules").toString())
		                                .set("RelevantWords", prop.get("relevantWords").toString())
		                                .set("OutputFile", prop.get("output").toString());
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Carica le regole di normalizzazione
		JavaPairRDD<String, String> normRules = sc.textFile(conf.get("NormRules"))
				                                  .mapToPair(new NormRulePairMapper("="))
				                                  .cache();
		
//		//Carica la lista di parole rilevanti
//		JavaRDD<String> relevantWords = sc.textFile(conf.get("RelevantWords"))
//				                          .cache();
		
		//Carica i tweet da normalizzare
		JavaPairRDD<Integer, TweetTrainingExample> tweetMap = sc.textFile(conf.get("Tweets"))
				                                            .map(new ExampleMapper("\".+\""))
				                                            .mapToPair(new PopKeyFunction<Integer, TweetTrainingExample>("id"))
				                                            .sortByKey()
				                                            .cache();
		
		//Spezza i tweet in parole
		JavaPairRDD<String, TweetWord> tweetWordsMap = tweetMap.map(new GetPairValueFunction<Integer, TweetTrainingExample>())
				                                               .flatMap(new TweetWordMapper())
				                                               .mapToPair(new PopKeyFunction<String, TweetWord>("word"));
		
		//Normalizza le parole secondo le regole
		JavaPairRDD<Integer, Iterable<TweetWord>> id2NormWords = tweetWordsMap.join(normRules)
				                                                              .map(new GetPairValueFunction<String, Tuple2<TweetWord, String>>())
				                                                              .map(new WordNormalizerMapper())
				                                                              .mapToPair(new PopKeyFunction<Integer, TweetWord>("tweetId"))
				                                                              .groupByKey();
		
		//Ricostruisci i tweet normalizzati
		JavaRDD<TweetTrainingExample> normalizedTweets = tweetMap.join(id2NormWords)
				                                                 .map(new GetPairValueFunction<Integer, Tuple2<TweetTrainingExample, Iterable<TweetWord>>>())
				                                                 .map(new TweetNormalizerMapper());
		
		normalizedTweets.saveAsTextFile(conf.get("OutputFile"));
	}
}
