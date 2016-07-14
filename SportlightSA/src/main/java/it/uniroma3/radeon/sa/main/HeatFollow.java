package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.Comment;
import it.uniroma3.radeon.sa.data.Post;
import it.uniroma3.radeon.sa.functions.DivideFunction;
import it.uniroma3.radeon.sa.functions.FieldContainsFunction;
import it.uniroma3.radeon.sa.functions.FieldExtractFunction;
import it.uniroma3.radeon.sa.functions.FlattenFunction;
import it.uniroma3.radeon.sa.functions.GetPairValueFunction;
import it.uniroma3.radeon.sa.functions.mappers.FrequencyStateMapper;
import it.uniroma3.radeon.sa.functions.mappers.PostMapper;
import it.uniroma3.radeon.sa.functions.stateful.StatefulAggregator;
import it.uniroma3.radeon.sa.functions.stateful.SumAggregator;
import it.uniroma3.radeon.sa.utils.Parsing;
import it.uniroma3.radeon.sa.utils.PropertyLoader;
import it.uniroma3.radeon.sa.utils.StateMaker;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class HeatFollow {
	
	public static void main(String[] args) {
		String configFile = args[0];
		Integer timeout = Integer.parseInt(args[1]);
		String toFollow = args[2];
		
		Properties prop = PropertyLoader.loadProperties(configFile);
		
		SparkConf conf = new SparkConf().setAppName("Heat following")
		                                .set("ZKQuorum", prop.get("zkQuorum").toString())
		                                .set("ConsumerGroupID", prop.get("consumerGroupID").toString())
		                                .set("Topics", prop.get("topicList").toString());
		
		JavaStreamingContext stsc = new JavaStreamingContext(conf, Durations.seconds(2));
		stsc.checkpoint("s3://sportlightstorage/checkpointing");
		
		//Definisci lo stato iniziale
		JavaPairRDD<String, Long> initialRDD = new StateMaker<Long>(stsc.sparkContext())
				                                   .setInitialEntry("postings", 0L)
				                                   .setInitialEntry("time", 0L)
				                                   .makeState();
		
		Map<String, Integer> topics = Parsing.parseTopics(conf.get("Topics"), ",", "/");
		
		//Crea uno stream di post e commenti dalla coda Kafka
		JavaDStream<Post> listenedPosts =
				KafkaUtils.createStream(stsc, conf.get("ZKQuorum"), conf.get("ConsumerGroupID"), topics)
				          .map(new GetPairValueFunction<String, String>())
				          .map(new PostMapper());
		
		//Filtra i post ascoltati mantenendo solo quelli relativi all'argomento da seguire, specificato come parametro
		JavaDStream<Post> followedPosts = listenedPosts.filter(new FieldContainsFunction<Post, String>("topics", toFollow));
		//Ottieni dai post una collezione del testo del post e di quello dei commenti associati
		JavaDStream<String> allPostTexts = followedPosts.map(new FieldExtractFunction<Post, String>("body"));
		
		JavaDStream<String> allCommentTexts = followedPosts.map(new FieldExtractFunction<Post, List<Comment>>("comments"))
				                                           .flatMap(new FlattenFunction<Comment>())
				                                           .map(new FieldExtractFunction<Comment, String>("body"));
		
		//Unisci le collezioni dei testi dei post e dei commenti e conta il numero
		JavaDStream<Long> noOfPostings = allPostTexts.union(allCommentTexts)
				                                     .count();
		
		//Crea una collezione che rappresenta la nuova osservazione di frequenza compiuta
		JavaPairDStream<String, Long> newFrequencyState = noOfPostings.flatMapToPair(new FrequencyStateMapper("postings", 2000L));
		
		//Funzione di aggiornamento per lo stato
		StatefulAggregator<String, Long> updateFunction = new SumAggregator<String>();
		
		//Aggiorna lo stato precedente
		JavaMapWithStateDStream<String, Long, Long, Tuple2<String, Long>> updates =
				newFrequencyState.mapWithState(StateSpec.function(updateFunction).initialState(initialRDD));
		
		JavaPairDStream<String, Long> updatedFrequencyState = updates.stateSnapshots();
		
		updatedFrequencyState.foreachRDD(new DivideFunction("postings", "time"));
		
		stsc.start();
		stsc.awaitTerminationOrTimeout(timeout);
		stsc.close();
	}
}