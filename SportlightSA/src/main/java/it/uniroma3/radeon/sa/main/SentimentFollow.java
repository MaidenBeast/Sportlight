package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.ClassificationResult;
import it.uniroma3.radeon.sa.data.Comment;
import it.uniroma3.radeon.sa.data.Post;
import it.uniroma3.radeon.sa.data.UnlabeledExample;
import it.uniroma3.radeon.sa.functions.FieldContainsFunction;
import it.uniroma3.radeon.sa.functions.FieldExtractFunction;
import it.uniroma3.radeon.sa.functions.FlattenFunction;
import it.uniroma3.radeon.sa.functions.GetPairValueFunction;
import it.uniroma3.radeon.sa.functions.PairToFunction;
import it.uniroma3.radeon.sa.functions.SumReduceFunction;
import it.uniroma3.radeon.sa.functions.mappers.ClassificationMapper;
import it.uniroma3.radeon.sa.functions.mappers.ClassificationMapper2;
import it.uniroma3.radeon.sa.functions.mappers.PostMapper;
import it.uniroma3.radeon.sa.functions.mappers.UnlabeledExampleMapper;
import it.uniroma3.radeon.sa.functions.mappers.UnlabeledTweetMapper;
import it.uniroma3.radeon.sa.functions.modifiers.VectorizerModifier;
import it.uniroma3.radeon.sa.functions.stateful.ConditionalDiffAggregator;
import it.uniroma3.radeon.sa.functions.stateful.StatefulAggregator;
import it.uniroma3.radeon.sa.functions.stateful.SumAggregator;
import it.uniroma3.radeon.sa.utils.Parsing;
import it.uniroma3.radeon.sa.utils.PropertyLoader;
import it.uniroma3.radeon.sa.utils.StateMaker;

import java.util.ArrayList;
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
	
	public static void main(String[] args) {
		String configFile = args[0];
		Integer timeout = Integer.parseInt(args[1]);
		String toFollow = args[2];
		
		Properties prop = PropertyLoader.loadProperties(configFile);
		
		SparkConf conf = new SparkConf().setAppName("Sentiment following")
										.set("NormRulesFile", prop.get("normRulesFile").toString())
										.set("ModelInputDir", prop.get("modelInputDir").toString())
		                                .set("ZKQuorum", prop.get("zkQuorum").toString())
		                                .set("ConsumerGroupID", prop.get("consumerGroupID").toString())
		                                .set("Topics", prop.get("topicList").toString());
		
		Map<String, String> normRules = Parsing.ruleParser(conf.get("NormRulesFile"), "=");
		
		JavaStreamingContext stsc = new JavaStreamingContext(conf, Durations.seconds(2));
		stsc.checkpoint("s3://sportlightstorage/checkpointing");
		
		//Definizione dello stato iniziale
		JavaPairRDD<String, Long> initialRDD = new StateMaker<Long>(stsc.sparkContext())
				                                   .setInitialEntry("neg", 0L)
				                                   .setInitialEntry("pos", 0L)
				                                   .makeState();
		
		Map<String, Integer> topics = Parsing.parseTopics(conf.get("Topics"), ",", "/");
		
		//Crea uno stream di post e commenti dalla coda Kafka
		JavaDStream<Post> listenedPosts =
				KafkaUtils.createStream(stsc, conf.get("ZKQuorum"), conf.get("ConsumerGroupID"), topics)
				          .map(new GetPairValueFunction<String, String>())
				          .map(new PostMapper());
		
		//Crea un convertitore che traduca ogni testo in una rappresentazione vettoriale
		HashingTF htf = new HashingTF(1000);
		
		//Filtra i post ascoltati mantenendo solo quelli relativi all'argomento prescelto
		JavaDStream<Post> followedPosts = listenedPosts.filter(new FieldContainsFunction<Post, String>("topics", toFollow));
		
		//Ottieni dai post una collezione del testo del post e di quello dei commenti associati
		JavaDStream<String> allPostTexts = followedPosts.map(new FieldExtractFunction<Post, String>("body", ""));
		
		JavaDStream<String> allCommentTexts = followedPosts.map(new FieldExtractFunction<Post, List<Comment>>("comments", new ArrayList<Comment>()))
				                                           .flatMap(new FlattenFunction<Comment>())
				                                           .map(new FieldExtractFunction<Comment, String>("body", ""));
		
		//Unisci le collezioni dei testi dei post e dei commenti ed effettua la normalizzazione
		JavaDStream<UnlabeledExample> normClassSet = allPostTexts.union(allCommentTexts)
				                                                 .map(new UnlabeledExampleMapper(normRules));
		
		//Calcola una rappresentazione vettoriale dei testi
		JavaDStream<UnlabeledExample> vsmClassSet = normClassSet.map(new VectorizerModifier(htf));
		
		//Carica il modello di classificazione
		NaiveBayesModel model = NaiveBayesModel.load(stsc.sparkContext().sc(), "s3://" + conf.get("ModelInputDir"));
		
		//Classifica i tweet per sentimento utilizzando il modello
		JavaDStream<String> classifiedSet = vsmClassSet.map(new ClassificationMapper2(model));
		
		//Conta i tweet classificati per sentimento
		JavaPairDStream<String, Long> sentiment2count = classifiedSet.countByValue();
		
		//Definisci la funzione di aggiornamento
		StatefulAggregator<String, Long> updateFunction = new ConditionalDiffAggregator<String>()
				                                              .withNegativeKey("neg");
		
		//Aggiorna lo stato precedente
		JavaMapWithStateDStream<String, Long, Long, Tuple2<String, Long>> updates =
				sentiment2count.mapWithState(StateSpec.function(updateFunction).initialState(initialRDD));
		
		//Ottieni lo stato attuale ed esegui la differenza tra post/commenti positivi e post/commenti negativi
		//E' sufficiente la somma algebrica perchè lo stato è aggiornato in maniera che il valore associato ai post negativi sia negativo in segno
		JavaPairDStream<String, Long> totals = updates.stateSnapshots();
		
		JavaDStream<Long> sentValue = totals.map(new GetPairValueFunction<String, Long>())
				                            .reduce(new SumReduceFunction());
		
		sentValue.print();
		
		stsc.start();
		stsc.awaitTerminationOrTimeout(timeout);
		stsc.close();
	}
}