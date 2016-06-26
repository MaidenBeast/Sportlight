package it.uniroma3.radeon.sa.main;

import java.io.FileReader;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.HashingTF;

public class Train {
	
	public static void main(String[] args) {
		String trainingFile = args[0];
		String configFile = args[1];
		
		Properties prop = new Properties();
		try {
			prop.load(new FileReader(configFile));
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("Sentiment Analysis trainer")
				                        .set("NormRules", prop.get("normRules").toString())
		                                .set("RelevantWords", prop.get("relevantWords").toString());
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Conversione da documento a vettore di occorrenze
		HashingTF htf = new HashingTF(50000);
		
		//Carica le regole di normalizzazione
//		JavaRDD<NormalizationRule> normRules = sc.textFile(conf.get("NormRules"));
	}

}
