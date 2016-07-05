package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.ClassificationResult;
import it.uniroma3.radeon.sa.data.UnlabeledTweet;
import it.uniroma3.radeon.sa.functions.FieldExtractFunction;
import it.uniroma3.radeon.sa.functions.GetPairValueFunction;
import it.uniroma3.radeon.sa.functions.PairToFunction;
import it.uniroma3.radeon.sa.functions.SumReduceFunction;
import it.uniroma3.radeon.sa.functions.aggregators.SumToMapAggregator;
import it.uniroma3.radeon.sa.functions.mappers.ClassificationMapper;
import it.uniroma3.radeon.sa.functions.mappers.UnlabeledTweetMapper;
import it.uniroma3.radeon.sa.functions.modifiers.VectorizerModifier;
import it.uniroma3.radeon.sa.utils.Parsing;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class ClassifyKafka {
	
	public static void main(String[] args) {
		String configFile = args[0];
		Integer timeout = Integer.parseInt(args[1]);
		
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
		                                .set("ResultOutputDir", prop.get("resultOutputDir").toString())
		                                .set("ZKQuorum", prop.get("zkQuorum").toString())
		                                .set("ConsumerGroupID", prop.get("consumerGroupID").toString())
		                                .set("Topics", prop.get("topicList").toString());
		
		
		JavaStreamingContext stsc = new JavaStreamingContext(conf, Durations.seconds(2));
		
		//Crea un convertitore che traduca ogni tweet in una rappresentazione vettoriale
		HashingTF htf = new HashingTF(1000);
		
		//Crea uno stream di tweet da classificare dalla coda Kafka
		JavaDStream<String> listenedTweets =
				KafkaUtils.createStream(stsc, conf.get("ZKQuorum"), conf.get("ConsumerGroupID"), null)
				          .map(new GetPairValueFunction<String, String>());
		
		//Normalizza i tweet da classificare
		JavaDStream<UnlabeledTweet> normClassSet = listenedTweets.map(new UnlabeledTweetMapper(",", translationRules));
		
		//Calcola una rappresentazione vettoriale dei tweet da classificare
		JavaDStream<UnlabeledTweet> vsmClassSet = normClassSet.map(new VectorizerModifier(htf));
		
		//Carica il modello di classificazione
		NaiveBayesModel model = NaiveBayesModel.load(stsc.sparkContext().sc(), "file://" + conf.get("ModelInputDir"));
		
		//Classifica i tweet per sentimento utilizzando il modello
		JavaDStream<ClassificationResult> classifiedSet = vsmClassSet.map(new ClassificationMapper(model));
		
		//Conta i tweet classificati per sentimento
		JavaPairDStream<String, Integer> sentiment2count = classifiedSet.map(new FieldExtractFunction<ClassificationResult, String>("sentiment"))
				                                                        .mapToPair(new PairToFunction<String, Integer>(1))
				                                                        .reduceByKey(new SumReduceFunction());
		
		Map<String, Integer> totals = new HashMap<>();
		sentiment2count.foreachRDD(new SumToMapAggregator(totals));
		
		stsc.start();
		stsc.awaitTerminationOrTimeout(timeout);
		System.out.println(totals);
	}
}
