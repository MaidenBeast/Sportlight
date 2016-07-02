package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.TweetExample;
import it.uniroma3.radeon.sa.data.TweetWord;
import it.uniroma3.radeon.sa.functions.ConcatFunction;
import it.uniroma3.radeon.sa.functions.GetPairValueFunction;
import it.uniroma3.radeon.sa.functions.PopKeyFunction;
import it.uniroma3.radeon.sa.functions.mappers.ExampleMapper;
import it.uniroma3.radeon.sa.functions.mappers.NormRulePairMapper;
import it.uniroma3.radeon.sa.functions.mappers.TweetNormalizerMapper;
import it.uniroma3.radeon.sa.functions.mappers.TweetWordMapper;
import it.uniroma3.radeon.sa.functions.mappers.WordNormalizerMapper;
import it.uniroma3.radeon.sa.utils.Parsing;

import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Optional;

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
		
		//Carica le regole di normalizzazione. Saranno inviate dentro le funzioni ai nodi del cluster
		Map<String, String> normRules = Parsing.ruleParser(prop.get("normRules").toString(), "=");
		
		//Carica la lista di parole rilevanti
		//List<String> relevantWords = Parsing.wordListParser(prop.get("relevantWords").toString())
		
		SparkConf conf = new SparkConf().setAppName("Sentiment Analysis normalizer")
										.set("RawTweetsFile", prop.get("tweets").toString())
		                                .set("OutputDir", prop.get("output").toString());
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
//		//Carica le regole di normalizzazione
//		JavaPairRDD<String, String> normRules = sc.textFile("file://" + conf.get("NormRules"))
//				                                  .mapToPair(new NormRulePairMapper("="))
//				                                  .cache();
//		
////		//Carica la lista di parole rilevanti
////		JavaRDD<String> relevantWords = sc.textFile("file://" + conf.get("RelevantWords"))
////				                          .cache();
//		
//		//Carica i tweet da normalizzare
//		JavaPairRDD<Integer, TweetExample> tweetMap = sc.textFile("file://" + conf.get("Tweets"))
//				                                            .map(new ExampleMapper(","))
//				                                            .mapToPair(new PopKeyFunction<Integer, TweetExample>("id"))
//				                                            .sortByKey()
//				                                            .cache();
//		
//		//Spezza i tweet in parole
//		JavaPairRDD<String, TweetWord> tweetWordsMap = tweetMap.map(new GetPairValueFunction<Integer, TweetExample>())
//				                                               .flatMap(new TweetWordMapper())
//				                                               .mapToPair(new PopKeyFunction<String, TweetWord>("word"));
//		
//		//Normalizza le parole secondo le regole
//		JavaPairRDD<Integer, Iterable<TweetWord>> id2NormWords = tweetWordsMap.leftOuterJoin(normRules)
//				                                                              .map(new GetPairValueFunction<String, Tuple2<TweetWord, Optional<String>>>())
//				                                                              .map(new WordNormalizerMapper())
//				                                                              .mapToPair(new PopKeyFunction<Integer, TweetWord>("tweetId"))
//				                                                              .groupByKey();
//		
//		//Ricostruisci i tweet normalizzati
//		JavaRDD<TweetExample> normalizedTweets = tweetMap.join(id2NormWords)
//				                                                 .map(new GetPairValueFunction<Integer, Tuple2<TweetExample, Iterable<TweetWord>>>())
//				                                                 .map(new TweetNormalizerMapper());
//		
//		normalizedTweets.saveAsTextFile("file://" + conf.get("OutputFile"));
		sc.close();
	}
}
