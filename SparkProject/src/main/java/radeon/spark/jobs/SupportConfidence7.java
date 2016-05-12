package radeon.spark.jobs;

import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import radeon.spark.data.ProductPair;
import radeon.spark.functions.IterativeSumFunction;
import radeon.spark.parsing.BillParser;
import radeon.spark.utils.ProductPairs;
import scala.Tuple2;

public class SupportConfidence7 {
	
	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("SupportConfidence");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		long startTime = System.currentTimeMillis();
		
		JavaRDD<String> bills = sc.textFile(inputPath).cache();
		final int totalBills = (int) bills.count();
		
		final BillParser parser = new BillParser();
		JavaPairRDD<String, Integer> billsWithProduct =
				bills.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Iterable<Tuple2<String, Integer>> call(String bill) {
						Set<Tuple2<String, Integer>> products = parser.parseBillProductSales(bill);
						return products;
					}
				}).reduceByKey(new IterativeSumFunction())
				  .cache();
		
		JavaPairRDD<ProductPair, Integer> billsWithPair =
				bills.flatMapToPair(new PairFlatMapFunction<String, ProductPair, Integer>() {
					private static final long serialVersionUID = 1L;
					
					public Iterable<Tuple2<ProductPair, Integer>> call(String bill) {
						List<Tuple2<ProductPair, Integer>> productPairs = parser.parsePairs(bill);
						return productPairs;
					}
				}).reduceByKey(new IterativeSumFunction());
		
		//Estrai il primo membro della coppia. Necessario per avere disponibile il conteggio degli scontrini che contengono tale primo membro
		JavaPairRDD<String, Tuple2<ProductPair, Integer>> left2billsWithPair =
				billsWithPair.mapToPair(new PairFunction<Tuple2<ProductPair, Integer>, String, Tuple2<ProductPair, Integer>>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Tuple2<ProductPair, Integer>> call(Tuple2<ProductPair, Integer> pair) {
						return new Tuple2<>(pair._1().getLeft(), pair);
					}
				});
		
		//Il join permetterà di affiancare al conteggio per ciascuna coppia il conteggio relativo solo al primo membro
		//Struttura delle coppie risultanti: (prodotto, (conteggioProdotto, (coppia, conteggioCoppia)))
		JavaPairRDD<String, Tuple2<Integer, Tuple2<ProductPair, Integer>>> billsWithProductAndPairJoined =
				billsWithProduct.join(left2billsWithPair);
		
		JavaPairRDD<Integer, Tuple2<ProductPair, Integer>> billsWithProductAndPair =
				billsWithProductAndPairJoined.mapToPair(
						new PairFunction< Tuple2<String, Tuple2<Integer, Tuple2<ProductPair, Integer>>>, Integer, Tuple2<ProductPair, Integer>>() {
							private static final long serialVersionUID = 1L;

							public Tuple2<Integer, Tuple2<ProductPair, Integer>> call(Tuple2<String, Tuple2<Integer, Tuple2<ProductPair, Integer>>> element) {
								return element._2();
							}
						});
		
		JavaRDD<ProductPair> result =
				billsWithProductAndPair.map(new Function<Tuple2<Integer, Tuple2<ProductPair, Integer>>, ProductPair>() {
					private static final long serialVersionUID = 1L;

					public ProductPair call(Tuple2<Integer, Tuple2<ProductPair, Integer>> element) {
						return ProductPairs.calculateSupportAndConfidence(element._2()._1(), element._2()._2(), totalBills, element._1());
					}
				});
		
		result.saveAsTextFile(outputPath);
		double totalTime = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Total elapsed time: " + totalTime);
		sc.close();
	}
}
