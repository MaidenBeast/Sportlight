package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.Post;
import it.uniroma3.radeon.sa.functions.FieldExtractFunction;
import it.uniroma3.radeon.sa.functions.FlattenFunction;
import it.uniroma3.radeon.sa.functions.GetPairValueFunction;
import it.uniroma3.radeon.sa.functions.PairToFunction;
import it.uniroma3.radeon.sa.functions.SumReduceFunction;
import it.uniroma3.radeon.sa.functions.mappers.PostMapper;
import it.uniroma3.radeon.sa.functions.stateful.StatefulAggregator;
import it.uniroma3.radeon.sa.functions.stateful.SumAggregator;
import it.uniroma3.radeon.sa.utils.PropertyLoader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class TrendAnalysis {
	
	public static void main(String[] args) {
		String configFile = args[0];
		Integer timeout = Integer.parseInt(args[1]);
		
		Properties prop = PropertyLoader.loadProperties(configFile);
		
		SparkConf conf = new SparkConf().setAppName("Most popular topics")
		                                .set("ZKQuorum", prop.get("zkQuorum").toString())
		                                .set("ConsumerGroupID", prop.get("consumerGroupID").toString())
		                                .set("Topics", prop.get("topicList").toString());
		
		
		JavaStreamingContext stsc = new JavaStreamingContext(conf, Durations.seconds(2));
		stsc.checkpoint("s3://sportlightstorage/checkpointing");
		
		Map<String, Integer> topics = new HashMap<>();
		topics.put("tweets", 1);
		
		//Crea uno stream di post dalla coda Kafka
		JavaDStream<Post> listenedPosts =
				KafkaUtils.createStream(stsc, conf.get("ZKQuorum"), conf.get("ConsumerGroupID"), topics)
				          .map(new GetPairValueFunction<String, String>())
				          .map(new PostMapper());
		
		//Per ogni post preleva e conta gli argomenti
		JavaPairDStream<String, Long> topic2count = listenedPosts.map(new FieldExtractFunction<Post, List<String>>("topics"))
				                                                    .flatMap(new FlattenFunction<String>())
				                                                    .mapToPair(new PairToFunction<String, Long>(1L))
				                                                    .reduceByKey(new SumReduceFunction());
		
		//Definisci la funzione di aggiornamento
		StatefulAggregator<String, Long> updateFunction = new SumAggregator<String>();
		//Aggiorna lo stato precedente e ordina per conteggio
		JavaMapWithStateDStream<String, Long, Long, Tuple2<String, Long>> totals = 
				topic2count.mapWithState(StateSpec.function(updateFunction));
//				           .map(new ReversePairFunction())
//				           .sortByKey()
//				           .map(new ReversePairFunction());
		
		//Automaticamente questa stampa prenderà i primi 10 elementi di totals
		totals.print();
		
		stsc.start();
		stsc.awaitTerminationOrTimeout(timeout);
	}
}