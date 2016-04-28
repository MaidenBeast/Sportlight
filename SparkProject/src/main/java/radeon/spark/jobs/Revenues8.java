package radeon.spark.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import radeon.spark.data.MonthProductKey;
import radeon.spark.data.Product;
import radeon.spark.data.comparators.CountComparator;
import radeon.spark.functions.IterativeSumFunction;
import radeon.spark.parsing.BillParser;
import radeon.spark.parsing.CostsParser;
import radeon.spark.utils.Products;
import scala.Tuple2;

public class Revenues8 {
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		String inputPath = args[0];
		String costPath = "/costs.properties";
		String outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Revenues");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Prepara i costi
		CostsParser costParser = new CostsParser();
		JavaPairRDD<String, Integer> prod2cost =
				sc.textFile(costPath)
				  .mapToPair(line -> costParser.parseCosts(line))
				  .cache();
		
		BillParser billParser = new BillParser();
		JavaPairRDD<String, MonthProductKey> product2sale =
				sc.textFile(inputPath)
				  .flatMap(line -> billParser.parseBillKeysOnly(line))
				  .mapToPair(monthProduct -> monthProduct.toTupleKeyProduct());
		
		JavaPairRDD<MonthProductKey, Integer> salesWithCosts =
				product2sale.join(prod2cost)
				            .mapToPair(joined -> joined._2());
		
		JavaPairRDD<MonthProductKey, Integer> productMonthRevenues =
				salesWithCosts.reduceByKey((a, b) -> a + b);
		
		JavaPairRDD<String, Tuple2<String, Integer>> product2monthRevenue =
				productMonthRevenues.mapToPair(revenue -> new Tuple2<>(revenue._1().getProduct(),
						                                               new Tuple2<>(revenue._1().getMonth(), revenue._2())));
				
//		
//		JavaPairRDD<String, MonthProductKey> allSales =
//				sc.textFile(inputPath).flatMap(new FlatMapFunction<String, MonthProductKey>() {
//					public Iterable<MonthProductKey> call(String s) {
//						BillParser parser = new BillParser();
//						List<MonthProductKey> saleKeys = parser.parseBillKeysOnly(s);
//						return saleKeys;
//					}
//				})
//				.mapToPair(new PairFunction<MonthProductKey, String, MonthProductKey>() {
//					public Tuple2<String, MonthProductKey> call(MonthProductKey mpk) {
//						return new Tuple2<>(mpk.getProduct(), mpk);
//					}
//				});
//		
//		JavaPairRDD<MonthProductKey, Integer> salesWithCosts =
//				allSales.join(prod2cost)
//				        .mapToPair(new PairFunction<Tuple2<String, Tuple2<MonthProductKey, Integer>>, MonthProductKey, Integer>() {
//				        	public Tuple2<MonthProductKey, Integer> call(Tuple2<String, Tuple2<MonthProductKey, Integer>> joined) {
//				        		MonthProductKey key = joined._2()._1();
//				        		Integer cost = joined._2()._2();
//				        		return new Tuple2<>(key, cost);
//				        	}
//				        });
//		
//		JavaPairRDD<MonthProductKey, Integer> productMonthRevenues =
//				salesWithCosts.reduceByKey(new IterativeSumFunction());
//		
//		JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> monthlyRevenuesPerProduct =
//				productMonthRevenues.mapToPair(new PairFunction<Tuple2<MonthProductKey, Integer>, String, Tuple2<String,Integer>>() {
//					public Tuple2<String, Tuple2<String,Integer>> call(Tuple2<MonthProductKey, Integer> pmRev) {
//						String product = pmRev._1().getProduct();
//						String month = pmRev._1().getMonth();
//						Integer revenue = pmRev._2();
//						return new Tuple2<>(product, new Tuple2<>(month, revenue));
//					}
//				}).groupByKey();
//		
//		monthlyRevenuesPerProduct.saveAsTextFile(outputPath);
		sc.close();
	}
}
