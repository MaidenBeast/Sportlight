package it.uniroma3.radeon.jobs;

import it.uniroma3.radeon.data.TweetData;
import it.uniroma3.radeon.functions.TweetFilteringFunction;
import it.uniroma3.radeon.functions.accessories.EqualsCondition;
import it.uniroma3.radeon.functions.accessories.TweetCondition;
import it.uniroma3.radeon.functions.mappers.TweetMapper;
import it.uniroma3.radeon.streaming.StreamLoader;
import it.uniroma3.radeon.streaming.StreamUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

public class Euro2016 implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) {
		StreamLoader sl = new StreamLoader(2, "Euro2016");
		JavaStreamingContext jssc = sl.initializeStream();
	    JavaDStream<Status> tweetsStream = loadData(jssc);
	    
	    //Prendi tutti i tweet degli Euro 2016 (tweet associati all'account UEFAEURO)
	    
	    JavaDStream<TweetData> tweets = tweetsStream.map(new TweetMapper());
	    
	    JavaDStream<TweetData> provaFilter = tweets.filter(new TweetFilteringFunction(new EqualsCondition("IsRetweet", true)));
	    
//	    JavaDStream<TweetData> euroTweets = tweets.filter(new TweetFilteringFunction("User.Name", "UEFAEURO"));
//	    
//	    JavaDStream<TweetData> retweeted = tweets.filter(new TweetFilteringFunction("IsRetweet", "true"))
//	    		                                 .filter(new TweetFilteringFunction("Retweeted.User.Name", "UEFAEURO"));
	    
	    final List<TweetData> sample = new ArrayList<>();
	    
	    //Preleva un campione dei tweet degli Euro 2016
	    provaFilter.foreachRDD(new VoidFunction<JavaRDD<TweetData>>() {
	    	
			private static final long serialVersionUID = 1L;

			public void call(JavaRDD<TweetData> rdd) {
	    		List<TweetData> first10 = rdd.take(10);
	    		for (TweetData td : first10) {
	    			sample.add(td);
	    		}
	    	}
	    });

	    jssc.start();
	    jssc.awaitTerminationOrTimeout(60 * 1000);
	    
	    System.out.println(sample);		
	}
	
	private static JavaDStream<Status> loadData(JavaStreamingContext jssc) {
		JavaDStream<Status> tweetsStream = TwitterUtils.createStream(jssc, StreamUtils.getAuth());
		return tweetsStream;
	}
}
