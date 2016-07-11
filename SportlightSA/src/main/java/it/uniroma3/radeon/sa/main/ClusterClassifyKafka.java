package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.ClassificationResult;
import it.uniroma3.radeon.sa.data.UnlabeledTweet;
import it.uniroma3.radeon.sa.data.shared.accumulators.SentimentCountAccumulator;
import it.uniroma3.radeon.sa.functions.FieldExtractFunction;
import it.uniroma3.radeon.sa.functions.GetPairValueFunction;
import it.uniroma3.radeon.sa.functions.PairToFunction;
import it.uniroma3.radeon.sa.functions.SumReduceFunction;
import it.uniroma3.radeon.sa.functions.mappers.ClassificationMapper;
import it.uniroma3.radeon.sa.functions.mappers.UnlabeledTweetMapper;
import it.uniroma3.radeon.sa.functions.modifiers.VectorizerModifier;
import it.uniroma3.radeon.sa.functions.stateful.SumAggregator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.VoidFunction;

import it.uniroma3.radeon.sa.utils.Parsing;
import it.uniroma3.radeon.sa.utils.PropertyLoader;

import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import scala.Tuple2;

public class ClusterClassifyKafka {
	
	public static void main(String[] args) {
		String configFile = args[0];
		Integer timeout = Integer.parseInt(args[1]);
		
		Properties prop = PropertyLoader.loadProperties(configFile);
		
		//Carica le regole di traduzione
		Map<String, String> translationRules = Parsing.ruleParser(prop.get("translationRules").toString(), "=");
		
		SparkConf conf = new SparkConf().setAppName("Sentiment Analysis classifier")
				                        .set("RawTweets", prop.get("rawTweets").toString())
										.set("ModelInputDir", prop.get("modelInputDir").toString())
		                                .set("ResultOutputDir", prop.get("resultOutputDir").toString())
		                                .set("ZKQuorum", prop.get("zkQuorum").toString())
		                                .set("ConsumerGroupID", prop.get("consumerGroupID").toString())
		                                .set("Topics", prop.get("topicList").toString());
		
		
		JavaStreamingContext stsc = new JavaStreamingContext(conf, Durations.seconds(2));
		stsc.checkpoint("s3://sportlightstorage/checkpointing");
		
		//Definizione dello stato iniziale
		List<Tuple2<String, Long>> tuples =
        	Arrays.asList(new Tuple2<>("0.0", 0L), new Tuple2<>("1.1", 0L));
		JavaPairRDD<String, Long> initialRDD = stsc.sparkContext().parallelizePairs(tuples);
		
		Map<String, Integer> topics = new HashMap<>();
		topics.put("tweets", 1);
		
		//Crea un convertitore che traduca ogni tweet in una rappresentazione vettoriale
		HashingTF htf = new HashingTF(1000);
		
		//Crea uno stream di tweet da classificare dalla coda Kafka
		JavaDStream<String> listenedTweets =
				KafkaUtils.createStream(stsc, conf.get("ZKQuorum"), conf.get("ConsumerGroupID"), topics)
				          .map(new GetPairValueFunction<String, String>());
		
//		listenedTweets.print();
		
		//Normalizza i tweet da classificare
		JavaDStream<UnlabeledTweet> normClassSet = listenedTweets.map(new UnlabeledTweetMapper(",", translationRules));
		
		//Calcola una rappresentazione vettoriale dei tweet da classificare
		JavaDStream<UnlabeledTweet> vsmClassSet = normClassSet.map(new VectorizerModifier(htf));
		
		//Carica il modello di classificazione
		NaiveBayesModel model = NaiveBayesModel.load(stsc.sparkContext().sc(), "s3://" + conf.get("ModelInputDir"));
		
		//Classifica i tweet per sentimento utilizzando il modello
		JavaDStream<ClassificationResult> classifiedSet = vsmClassSet.map(new ClassificationMapper(model));
		
		//Conta i tweet classificati per sentimento
		JavaPairDStream<String, Long> sentiment2count = classifiedSet.map(new FieldExtractFunction<ClassificationResult, String>("sentiment"))
				                                                        .mapToPair(new PairToFunction<String, Long>(1L))
				                                                        .reduceByKey(new SumReduceFunction());
		
		//Funzione di aggiornamento (nel refactor deve essere assolutamente tolta da qui)
	    Function3<String, Optional<Long>, State<Long>, Tuple2<String, Long>> updateFunc =
	            new Function3<String, Optional<Long>, State<Long>, Tuple2<String, Long>>() {
	    	
					private static final long serialVersionUID = 1L;

				@Override
	              public Tuple2<String, Long> call(String sentiment, Optional<Long> newCount,
	                  State<Long> prevCount) {
					long sum = newCount.or(0L) + (prevCount.exists() ? prevCount.get() : 0L);
	                Tuple2<String, Long> output = new Tuple2<>(sentiment, sum);
	                prevCount.update(sum);
	                return output;
	              }
	            };
		//Aggiorna lo stato precedente
		JavaMapWithStateDStream<String, Long, Long, Tuple2<String, Long>> totals = 
				sentiment2count.mapWithState(StateSpec.function(updateFunc).initialState(initialRDD));
		
		totals.print();
		
		stsc.start();
		stsc.awaitTerminationOrTimeout(timeout);
	}
}