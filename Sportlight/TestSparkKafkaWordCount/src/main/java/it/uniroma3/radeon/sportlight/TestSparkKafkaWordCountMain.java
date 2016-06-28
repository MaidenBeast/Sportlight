package it.uniroma3.radeon.sportlight;

import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

public class TestSparkKafkaWordCountMain {
	private static final Pattern SPACE = Pattern.compile(" ");

	private TestSparkKafkaWordCountMain() {}

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("TestSparkKafkaWordCountMain");
		// Create the context with 2 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

		int numThreads = 1; //single thread
		Map<String, Integer> topicMap = new HashMap<>();
		topicMap.put("sportlight", numThreads);

		JavaPairReceiverInputDStream<String, String> messages =
				KafkaUtils.createStream(jssc, "localhost", "my-consumer-group", topicMap);

		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String x) {
				return Lists.newArrayList(SPACE.split(x));
			}
		});

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		wordCounts.print();
		jssc.start();
		jssc.awaitTermination();

	}

}
