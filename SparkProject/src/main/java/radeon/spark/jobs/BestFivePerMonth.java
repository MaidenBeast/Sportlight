package radeon.spark.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import radeon.spark.data.MonthProductKey;
import radeon.spark.data.Product;
import radeon.spark.data.comparators.CountComparator;
import radeon.spark.parsing.BillParser;
import radeon.spark.utils.Products;
import scala.Tuple2;

public class BestFivePerMonth {
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Best five per month");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaPairRDD<MonthProductKey, Integer> allSales =
				sc.textFile(inputPath).flatMapToPair(new PairFlatMapFunction<String, MonthProductKey, Integer>() {
					public Iterable<Tuple2<MonthProductKey, Integer>> call(String s) {
						BillParser parser = new BillParser();
						List<Tuple2<MonthProductKey, Integer>> singleSales = parser.parseBill(s);
						return singleSales;
					}
		});
		
		JavaPairRDD<MonthProductKey, Integer> allSalesAggr = allSales.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer prevValue, Integer newValue) {
				return prevValue + newValue;
			}
		});
		
		JavaPairRDD<String, Product> month2ProductInfo = allSalesAggr.mapToPair(new PairFunction<Tuple2<MonthProductKey, Integer>, String, Product>() {
			public Tuple2<String, Product> call(Tuple2<MonthProductKey, Integer> aggr) {
				String month = aggr._1().getMonth();
				String product = aggr._1().getProduct();
				Integer productCount = aggr._2();
				return new Tuple2<>(month, new Product(product, productCount, 0));
			}
		});
		
		JavaPairRDD<String, Iterable<Product>> month2Top5 =
				month2ProductInfo.groupByKey()
				.mapToPair(new PairFunction<Tuple2<String, Iterable<Product>>, String, Iterable<Product>>() {
					public Tuple2<String, Iterable<Product>> call(Tuple2<String, Iterable<Product>> whole) {
						List<Product> best5 = new ArrayList<>();
						String month = whole._1();
						Iterable<Product> allProducts = whole._2();
						for (Product p : allProducts) {
							best5.add(p);
						}
						best5 = Products.takeBest(best5, 5, new CountComparator());
						Iterable<Product> best5Iterable = best5;
						return new Tuple2<>(month, best5Iterable);
					}
				});
		
		month2Top5.saveAsTextFile(outputPath);
		sc.close();
	}
}
