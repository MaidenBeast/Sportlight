package it.uniroma3.radeon.streaming;

import it.uniroma3.radeon.functions.IterativeSumFunction;
import it.uniroma3.radeon.functions.MatchFunction;
import it.uniroma3.radeon.functions.PairToFunction;
import it.uniroma3.radeon.functions.ReversePairFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

/**
 *  The Spark Streaming documentation is available on:
 *  http://spark.apache.org/docs/latest/streaming-programming-guide.html
 *
 *  Spark Streaming is an extension of the core Spark API that enables scalable,
 *  high-throughput, fault-tolerant stream processing of live data streams.
 *  Spark Streaming receives live input data streams and divides the data into batches,
 *  which are then processed by the Spark engine to generate the final stream of results in batches.
 *  Spark Streaming provides a high-level abstraction called discretized stream or DStream,
 *  which represents a continuous stream of data.
 *
 *  In this exercise we will:
 *  - Print the status text of the some of the tweets
 *  - Find the 10 most popular Hashtag  in the last 60 seconds
 *
 *  You can see informations about the streaming in the Spark UI console: http://localhost:4040/streaming/
 */
public class StreamingOnTweets {

  JavaStreamingContext jssc;
  /**
   *  Load the data using TwitterUtils: we obtain a DStream of tweets
   *
   *  More about TwitterUtils:
   *  https://spark.apache.org/docs/1.4.0/api/java/index.html?org/apache/spark/streaming/twitter/TwitterUtils.html
   */
  public JavaDStream<Status> loadData() {
    // create the spark configuration and spark context
    SparkConf conf = new SparkConf()
        .setAppName("Play with Spark Streaming");

    // create a java streaming context and define the window (2 seconds batch)
    jssc = new JavaStreamingContext(conf, Durations.seconds(2));

    System.out.println("Initializing Twitter stream...");

    // create a DStream (sequence of RDD). The object tweetsStream is a DStream of tweet statuses:
    // - the Status class contains all information of a tweet
    // See http://twitter4j.org/javadoc/twitter4j/Status.html
    JavaDStream<Status> tweetsStream = TwitterUtils.createStream(jssc, StreamUtils.getAuth());

    return tweetsStream;
  }

  /**
   *  Find the 10 most popular Hashtag in the last minute
   */
  public String top10Hashtag() {
    JavaDStream<Status> tweetsStream = loadData();

    // First, find all hashtags
    // stream is like a sequence of RDD so you can do all the operation you did in the first part of the hands-on
    JavaDStream<String> hashtags = tweetsStream.flatMap(
    		new FlatMapFunction<Status, String>() {
				private static final long serialVersionUID = 1L;

				public Iterable<String> call (Status tweet) {
    				return Arrays.asList(tweet.getText().split(" "));
    			}
    		})
    		.filter(new MatchFunction(1, 0, "#(\\w+)"));

    // Make a "wordcount" on hashtag
    // Reduce last 60 seconds of data
    JavaPairDStream<Integer, String> hashtagMention = hashtags.mapToPair(new PairToFunction<String, Integer>(1))
                                                              .reduceByKeyAndWindow(new IterativeSumFunction(), new Duration(60000))
                                                              .mapToPair(new ReversePairFunction<String, Integer>());

    // Then sort the hashtags
    JavaPairDStream<Integer, String> sortedHashtag =
    		hashtagMention.transformToPair(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
				private static final long serialVersionUID = 1L;

				public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> hashtagRDD) {
    				return hashtagRDD.sortByKey(false);
    			}
    		});

    // and return the 10 most populars
    final List<Tuple2<Integer, String>> top10 = new ArrayList<>();
    
    sortedHashtag.foreachRDD(new VoidFunction<JavaPairRDD<Integer, String>>() {
    	
		private static final long serialVersionUID = 1L;

		public void call(JavaPairRDD<Integer, String> rdd) {
    		List<Tuple2<Integer, String>> mostPopular = rdd.take(10);
    		top10.addAll(mostPopular);
    	}
    });

    // we need to tell the context to start running the computation we have setup
    // it won't work if you don't add this!
    jssc.start();
    jssc.awaitTerminationOrTimeout(60 * 1000);

    return "Most popular hashtag :" + top10;
  }
}
