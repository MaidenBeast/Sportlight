package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.ClassificationResult;
import it.uniroma3.radeon.sa.data.Comment;
import it.uniroma3.radeon.sa.data.Post;
import it.uniroma3.radeon.sa.data.UnlabeledTweet;
import it.uniroma3.radeon.sa.functions.FieldExtractFunction;
import it.uniroma3.radeon.sa.functions.FlattenFunction;
import it.uniroma3.radeon.sa.functions.GetPairValueFunction;
import it.uniroma3.radeon.sa.functions.PairToFunction;
import it.uniroma3.radeon.sa.functions.SumReduceFunction;
import it.uniroma3.radeon.sa.functions.mappers.ClassificationMapper;
import it.uniroma3.radeon.sa.functions.mappers.PostMapper;
import it.uniroma3.radeon.sa.functions.mappers.UnlabeledTweetMapper;
import it.uniroma3.radeon.sa.functions.modifiers.VectorizerModifier;
import it.uniroma3.radeon.sa.functions.stateful.ConditionalDiffAggregator;
import it.uniroma3.radeon.sa.functions.stateful.StatefulAggregator;
import it.uniroma3.radeon.sa.functions.stateful.SumAggregator;
import it.uniroma3.radeon.sa.utils.Parsing;
import it.uniroma3.radeon.sa.utils.PropertyLoader;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class SentimentFollow {
	
	//Provvisorio: calcola l'evoluzione del sentimento su TUTTI gli argomenti ascoltati
	
	public static void main(String[] args) {
		String configFile = args[0];
		Integer timeout = Integer.parseInt(args[1]);
//		String toFollow = args[2];
		
		Properties prop = PropertyLoader.loadProperties(configFile);
		
		SparkConf conf = new SparkConf().setAppName("Sentiment following")
										.set("NormRulesFile", prop.get("normRulesFile").toString())
										.set("ModelInputDir", prop.get("modelInputDir").toString())
		                                .set("ZKQuorum", prop.get("zkQuorum").toString())
		                                .set("ConsumerGroupID", prop.get("consumerGroupID").toString())
		                                .set("Topics", prop.get("topicList").toString());
		
		Map<String, String> normRules = Parsing.ruleParser(conf.get("normRulesFile"), "=");
		
		JavaStreamingContext stsc = new JavaStreamingContext(conf, Durations.seconds(2));
		stsc.checkpoint("s3://sportlightstorage/checkpointing");
		
		//Definizione dello stato iniziale
		List<Tuple2<String, Integer>> tuples =
        	Arrays.asList(new Tuple2<>("0.0", 0), new Tuple2<>("1.0", 0));
		JavaPairRDD<String, Integer> initialRDD = stsc.sparkContext().parallelizePairs(tuples);
		
		Map<String, Integer> topics = new HashMap<>();
		topics.put("tweets", 1);
		
		//Crea uno stream di post e commenti dalla coda Kafka
		JavaDStream<Post> listenedPosts =
				KafkaUtils.createStream(stsc, conf.get("ZKQuorum"), conf.get("ConsumerGroupID"), topics)
				          .map(new GetPairValueFunction<String, String>())
				          .map(new PostMapper());
		
		//Crea un convertitore che traduca ogni testo in una rappresentazione vettoriale
		HashingTF htf = new HashingTF(1000);
		
		//Ottieni dai post una collezione del testo del post e di quello dei commenti associati
		JavaDStream<String> allPostTexts = listenedPosts.map(new FieldExtractFunction<Post, String>("body"));
		
		JavaDStream<String> allCommentTexts = listenedPosts.map(new FieldExtractFunction<Post, List<Comment>>("comments"))
				                                           .flatMap(new FlattenFunction<Comment>())
				                                           .map(new FieldExtractFunction<Comment, String>("body"));
		
		//Unisci le collezioni dei testi dei post e dei commenti ed effettua la normalizzazione
		JavaDStream<UnlabeledTweet> normClassSet = allPostTexts.union(allCommentTexts)
				                                               .map(new UnlabeledTweetMapper(",", normRules));
		
		//Calcola una rappresentazione vettoriale dei testi
		JavaDStream<UnlabeledTweet> vsmClassSet = normClassSet.map(new VectorizerModifier(htf));
		
		//Carica il modello di classificazione
		NaiveBayesModel model = NaiveBayesModel.load(stsc.sparkContext().sc(), "s3://" + conf.get("ModelInputDir"));
		
		//Classifica i tweet per sentimento utilizzando il modello
		JavaDStream<ClassificationResult> classifiedSet = vsmClassSet.map(new ClassificationMapper(model));
		
		//Conta i tweet classificati per sentimento
		JavaPairDStream<String, Integer> sentiment2count = classifiedSet.map(new FieldExtractFunction<ClassificationResult, String>("sentiment"))
				                                                        .mapToPair(new PairToFunction<String, Integer>(1))
				                                                        .reduceByKey(new SumReduceFunction());
		
		//Definisci la funzione di aggiornamento
		StatefulAggregator<String, Integer> updateFunction = new ConditionalDiffAggregator<String>()
				                                                 .withCondition("negative", "0.0");
		//Aggiorna lo stato precedente
		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> totals = 
				sentiment2count.mapWithState(StateSpec.function(updateFunction).initialState(initialRDD));
		
		//Ottieni il valore di sentimento come differenza tra post/commenti positivi e post/commenti negativi
		JavaDStream<Integer> sentValue = totals.map(new GetPairValueFunction<String, Integer>())
				                               .reduce(new SumReduceFunction());
		
		//Stampa l'unico elemento di sentValue, vale a dire il valore di sentimento netto nel corso del tempo
		sentValue.print();
		
		stsc.start();
		stsc.awaitTerminationOrTimeout(timeout);
	}
}