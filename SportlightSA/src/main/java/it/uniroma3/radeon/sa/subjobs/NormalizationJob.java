package it.uniroma3.radeon.sa.subjobs;

import it.uniroma3.radeon.sa.data.TweetExample;
import it.uniroma3.radeon.sa.data.TweetWord;
import it.uniroma3.radeon.sa.functions.GetPairValueFunction;
import it.uniroma3.radeon.sa.functions.PopKeyFunction;
import it.uniroma3.radeon.sa.functions.mappers.ExampleMapper;
import it.uniroma3.radeon.sa.functions.mappers.NormRulePairMapper;
import it.uniroma3.radeon.sa.functions.mappers.TweetNormalizerMapper;
import it.uniroma3.radeon.sa.functions.mappers.TweetWordMapper;
import it.uniroma3.radeon.sa.functions.mappers.WordNormalizerMapper;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.google.common.base.Optional;

public class NormalizationJob extends SubJob {
	
	public NormalizationJob(JavaSparkContext context, SparkConf config) {
		super(context, config);
	}

	@Override
	public JavaRDD<?> execute() {
		//Carica le regole di normalizzazione
		JavaPairRDD<String, String> normRules = this.getContext().textFile("file://" + this.getConfig().get("NormRules"))
				                                  .mapToPair(new NormRulePairMapper("="))
				                                  .cache();
//		//Carica la lista di parole rilevanti
//		JavaRDD<String> relevantWords = sc.textFile("file://" + conf.get("RelevantWords"))
//				                          .cache();
		
		//Carica i tweet da normalizzare
		JavaPairRDD<Integer, TweetExample> tweetMap = this.getContext().textFile("file://" + this.getConfig().get("Tweets"))
				                                            .map(new ExampleMapper(","))
				                                            .mapToPair(new PopKeyFunction<Integer, TweetExample>("id"))
				                                            .sortByKey()
				                                            .cache();
		
		//Spezza i tweet in parole
		JavaPairRDD<String, TweetWord> tweetWordsMap = tweetMap.map(new GetPairValueFunction<Integer, TweetExample>())
				                                               .flatMap(new TweetWordMapper())
				                                               .mapToPair(new PopKeyFunction<String, TweetWord>("word"));
		
		//Normalizza le parole secondo le regole
		JavaPairRDD<Integer, Iterable<TweetWord>> id2NormWords = tweetWordsMap.leftOuterJoin(normRules)
				                                                              .map(new GetPairValueFunction<String, Tuple2<TweetWord, Optional<String>>>())
				                                                              .map(new WordNormalizerMapper())
				                                                              .mapToPair(new PopKeyFunction<Integer, TweetWord>("tweetId"))
				                                                              .groupByKey();
		
		//Ricostruisci i tweet normalizzati
		JavaRDD<TweetExample> normalizedTweets = tweetMap.join(id2NormWords)
				                                                 .map(new GetPairValueFunction<Integer, Tuple2<TweetExample, Iterable<TweetWord>>>())
				                                                 .map(new TweetNormalizerMapper());
		//Libera la memoria
		normRules.unpersist();
		tweetMap.unpersist();
		return normalizedTweets;
	}

}
