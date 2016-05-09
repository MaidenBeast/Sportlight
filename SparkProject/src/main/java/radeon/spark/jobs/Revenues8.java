package radeon.spark.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import radeon.spark.data.MonthProductKey;
import radeon.spark.data.MonthReport;
import radeon.spark.parsing.BillParser;
import radeon.spark.parsing.CostsParser;
import radeon.spark.utils.MonthReports;
import radeon.spark.data.comparators.MonthComparator;
import scala.Tuple2;

public class Revenues8 {
	
	public static void main(String[] args) {
		String inputPath = args[0];
		String costPath = "input/costs.properties";
		String outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Revenues");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		long startTime = System.currentTimeMillis();
		
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
		
		JavaPairRDD<String, Iterable<MonthReport>> result =
				product2monthRevenue.mapToPair(productMonth -> new Tuple2<>(productMonth._1(), new MonthReport(productMonth._2()._1(), productMonth._2()._2())))
				                    .groupByKey()
				                    .mapToPair(productMonths -> new Tuple2<>(productMonths._1(), MonthReports.orderReports(productMonths._2(),
				                    		                                                                               new MonthComparator())));
		
		result.saveAsTextFile(outputPath);
		double totalTime = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("Total elapsed time: " + totalTime);
		sc.close();
	}
}
