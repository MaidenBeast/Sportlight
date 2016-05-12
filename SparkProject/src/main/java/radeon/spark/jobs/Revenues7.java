package radeon.spark.jobs;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import radeon.spark.data.MonthProductKey;
import radeon.spark.data.MonthReport;
import radeon.spark.data.comparators.MonthComparator;
import radeon.spark.functions.IterativeSumFunction;
import radeon.spark.parsing.BillParser;
import radeon.spark.parsing.CostsParser;
import radeon.spark.utils.MonthReports;
import scala.Tuple2;

public class Revenues7 {
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		String inputPath = args[0];
		String costPath = "input/costs.properties";
		String outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Revenues");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		long startTime = System.currentTimeMillis();
		
		//Prepara i costi
		JavaPairRDD<String, Integer> prod2cost =
				sc.textFile(costPath).mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						CostsParser parser = new CostsParser();
						return parser.parseCosts(s);
					}
				}).cache();
		
		JavaPairRDD<String, MonthProductKey> allSales =
				sc.textFile(inputPath).flatMap(new FlatMapFunction<String, MonthProductKey>() {
					public Iterable<MonthProductKey> call(String s) {
						BillParser parser = new BillParser();
						List<MonthProductKey> saleKeys = parser.parseBillKeysOnly(s);
						return saleKeys;
					}
				})
				.mapToPair(new PairFunction<MonthProductKey, String, MonthProductKey>() {
					public Tuple2<String, MonthProductKey> call(MonthProductKey mpk) {
						return new Tuple2<>(mpk.getProduct(), mpk);
					}
				});
		
		JavaPairRDD<MonthProductKey, Integer> salesWithCosts =
				allSales.join(prod2cost)
				        .mapToPair(new PairFunction<Tuple2<String, Tuple2<MonthProductKey, Integer>>, MonthProductKey, Integer>() {
				        	public Tuple2<MonthProductKey, Integer> call(Tuple2<String, Tuple2<MonthProductKey, Integer>> joined) {
				        		MonthProductKey key = joined._2()._1();
				        		Integer cost = joined._2()._2();
				        		return new Tuple2<>(key, cost);
				        	}
				        });
		
		JavaPairRDD<MonthProductKey, Integer> productMonthRevenues =
				salesWithCosts.reduceByKey(new IterativeSumFunction());
		
		JavaPairRDD<String, Iterable<MonthReport>> monthlyRevenuesPerProduct =
				productMonthRevenues.mapToPair(new PairFunction<Tuple2<MonthProductKey, Integer>, String, MonthReport>() {
					public Tuple2<String, MonthReport> call(Tuple2<MonthProductKey, Integer> pmRev) {
						String product = pmRev._1().getProduct();
						String month = pmRev._1().getMonth();
						Integer revenue = pmRev._2();
						return new Tuple2<>(product, new MonthReport(month, revenue));
					}
				}).groupByKey()
				  .mapToPair(new PairFunction<Tuple2<String, Iterable<MonthReport>>,
						                      String, Iterable<MonthReport>>(){
					  public Tuple2<String, Iterable<MonthReport>>
					         call(Tuple2<String, Iterable<MonthReport>> toOrder) {
						  return new Tuple2<>(toOrder._1(), MonthReports.orderReports(toOrder._2(), new MonthComparator()));
					  }
				  });
		
		monthlyRevenuesPerProduct.saveAsTextFile(outputPath);
		double totalTime = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Total elapsed time: " + totalTime);
		sc.close();
	}
}
