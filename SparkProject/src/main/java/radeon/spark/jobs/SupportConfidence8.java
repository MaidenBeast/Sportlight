package radeon.spark.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import radeon.spark.data.MonthProductKey;
import radeon.spark.data.Product;
import radeon.spark.data.ProductPair;
import radeon.spark.data.comparators.CountComparator;
import radeon.spark.functions.IterativeSumFunction;
import radeon.spark.parsing.BillParser;
import radeon.spark.parsing.CostsParser;
import radeon.spark.utils.ProductPairs;
import radeon.spark.utils.Products;
import scala.Tuple2;

public class SupportConfidence8 {
	
	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("SupportConfidence");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> bills = sc.textFile(inputPath).cache();
		int totalBills = (int) bills.count();
		
		BillParser parser = new BillParser();
		JavaPairRDD<String, Integer> billsWithProduct =
				bills.flatMapToPair(bill -> parser.parseBillProductSales(bill))
				     .reduceByKey((a, b) -> a + b)
				     .cache();
		
		JavaPairRDD<ProductPair, Integer> billsWithPair =
				bills.flatMapToPair(bill -> parser.parsePairs(bill))
				     .reduceByKey((a, b) -> a + b);
		
		//Estrai il primo membro della coppia. Necessario per avere disponibile il conteggio degli scontrini che contengono tale primo membro
		JavaPairRDD<String, Tuple2<ProductPair, Integer>> left2billsWithPair =
				billsWithPair.mapToPair(pair -> new Tuple2<>(pair._1().getLeft(), pair));
		
		//Il join permetterà di affiancare al conteggio per ciascuna coppia il conteggio relativo solo al primo membro
		//Struttura delle coppie risultanti: (prodotto, (conteggioProdotto, (coppia, conteggioCoppia)))
		JavaPairRDD<String, Tuple2<Integer, Tuple2<ProductPair, Integer>>> billsWithProductAndPairJoined =
				billsWithProduct.join(left2billsWithPair);
		
		JavaPairRDD<Integer, Tuple2<ProductPair, Integer>> billsWithProductAndPair =
				billsWithProductAndPairJoined.mapToPair(element -> element._2());
		
		JavaRDD<ProductPair> result =
				billsWithProductAndPair
				.map(element -> ProductPairs.calculateSupportAndConfidence(element._2()._1(), element._2()._2(), totalBills, element._1()));
		
		result.saveAsTextFile(outputPath);
		sc.close();
	}
}
