package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.ClassificationResult;
import it.uniroma3.radeon.sa.data.Comment;
import it.uniroma3.radeon.sa.data.Post;
import it.uniroma3.radeon.sa.data.UnlabeledTweet;
import it.uniroma3.radeon.sa.data.stateful.StateEntry;
import it.uniroma3.radeon.sa.functions.FieldExtractFunction;
import it.uniroma3.radeon.sa.functions.FlattenFunction;
import it.uniroma3.radeon.sa.functions.GetPairValueFunction;
import it.uniroma3.radeon.sa.functions.MakePairFunction;
import it.uniroma3.radeon.sa.functions.PairToFunction;
import it.uniroma3.radeon.sa.functions.ReversePairFunction;
import it.uniroma3.radeon.sa.functions.SumReduceFunction;
import it.uniroma3.radeon.sa.functions.mappers.ClassificationMapper;
import it.uniroma3.radeon.sa.functions.mappers.FrequencyStateMapper;
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

public class HeatFollow {
	
	//Provvisorio: calcola la frequenza di posting su TUTTI gli argomenti ascoltati
	
	public static void main(String[] args) {
		String configFile = args[0];
		Integer timeout = Integer.parseInt(args[1]);
//		String toFollow = args[2];
		
		Properties prop = PropertyLoader.loadProperties(configFile);
		
		SparkConf conf = new SparkConf().setAppName("Heat following")
		                                .set("ZKQuorum", prop.get("zkQuorum").toString())
		                                .set("ConsumerGroupID", prop.get("consumerGroupID").toString())
		                                .set("Topics", prop.get("topicList").toString());
		
		JavaStreamingContext stsc = new JavaStreamingContext(conf, Durations.seconds(2));
		stsc.checkpoint("s3://sportlightstorage/checkpointing");
		
		//Definisci lo stato iniziale
		List<Tuple2<String, Long>> tuples =
	        	Arrays.asList(new Tuple2<>("postings", 0L), new Tuple2<>("time", 0L));
			JavaPairRDD<String, Long> initialRDD = stsc.sparkContext().parallelizePairs(tuples);
		
		Map<String, Integer> topics = new HashMap<>();
		topics.put("tweets", 1);
		
		//Crea uno stream di post e commenti dalla coda Kafka
		JavaDStream<Post> listenedPosts =
				KafkaUtils.createStream(stsc, conf.get("ZKQuorum"), conf.get("ConsumerGroupID"), topics)
				          .map(new GetPairValueFunction<String, String>())
				          .map(new PostMapper());
		
		//Ottieni dai post una collezione del testo del post e di quello dei commenti associati
		JavaDStream<String> allPostTexts = listenedPosts.map(new FieldExtractFunction<Post, String>("body"));
		
		JavaDStream<String> allCommentTexts = listenedPosts.map(new FieldExtractFunction<Post, List<Comment>>("comments"))
				                                           .flatMap(new FlattenFunction<Comment>())
				                                           .map(new FieldExtractFunction<Comment, String>("body"));
		
		//Unisci le collezioni dei testi dei post e dei commenti e conta il numero
		JavaDStream<Long> noOfPostings = allPostTexts.union(allCommentTexts)
				                                     .count();
		
		//Crea una collezione che rappresenta la nuova osservazione di frequenza compiuta
		JavaPairDStream<String, Long> newFrequencyState = noOfPostings.flatMap(new FrequencyStateMapper("postings", 2000L))
				                                                      .mapToPair(new MakePairFunction<StateEntry<Long>, String, Long>("stateKey", "stateValue"));
		
		//Funzione di aggiornamento per lo stato
		StatefulAggregator<String, Long> updateFunction = new SumAggregator<String>();
		
		//Aggiorna lo stato precedente
		JavaMapWithStateDStream<String, Long, Long, Tuple2<String, Long>> frequencyCount =
				newFrequencyState.mapWithState(StateSpec.function(updateFunction).initialState(initialRDD));
		
		stsc.start();
		stsc.awaitTerminationOrTimeout(timeout);
	}
}