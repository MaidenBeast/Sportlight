package it.uniroma3.radeon.sa.main;

import it.uniroma3.radeon.sa.data.TweetTrainingExample;
import it.uniroma3.radeon.sa.data.TweetWord;
import it.uniroma3.radeon.sa.functions.ConcatFunction;
import it.uniroma3.radeon.sa.functions.GetPairValueFunction;
import it.uniroma3.radeon.sa.functions.PopKeyFunction;
import it.uniroma3.radeon.sa.functions.mappers.ExampleMapper;
import it.uniroma3.radeon.sa.functions.mappers.LabeledPointMapper;
import it.uniroma3.radeon.sa.functions.mappers.NormRulePairMapper;
import it.uniroma3.radeon.sa.functions.mappers.TweetNormalizerMapper;
import it.uniroma3.radeon.sa.functions.mappers.TweetWordMapper;
import it.uniroma3.radeon.sa.functions.mappers.WordNormalizerMapper;

import java.io.FileReader;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;

import com.google.common.base.Optional;

import scala.Tuple2;

public class Train {
	
	public static void main(String[] args) {
		String configFile = args[0];
		
		Properties prop = new Properties();
		try {
			prop.load(new FileReader(configFile));
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("Sentiment Analysis trainer")
										.set("Tweets", prop.get("tweets").toString())
		                                .set("ModelOutput", prop.get("output").toString());
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Crea un convertitore che traduca ogni tweet in una rappresentazione vettoriale
		HashingTF htf = new HashingTF(1000);
		
		//Carica il training set
		JavaRDD<LabeledPoint> trainingSet = sc.textFile("file://" + conf.get("Tweets"))
				                              .map(new LabeledPointMapper(",", htf));
		
		//Dividi il training set in training e test
		JavaRDD<LabeledPoint>[] splitSet = trainingSet.randomSplit(new double[]{0.6, 0.4}, 11L);
		sc.close();
	}
}
