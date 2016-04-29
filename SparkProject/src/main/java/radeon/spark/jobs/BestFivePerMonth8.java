package radeon.spark.jobs;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import radeon.spark.data.MonthProductKey;
import radeon.spark.data.ProductReport;
import radeon.spark.data.comparators.ReportComparator;
import radeon.spark.parsing.BillParser;
import radeon.spark.utils.ProductReports;
import scala.Tuple2;

public class BestFivePerMonth8 {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Best five per month");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		BillParser parser = new BillParser();
		JavaPairRDD<MonthProductKey, Integer> allSales =
				sc.textFile(inputPath)
				  .flatMapToPair(line -> parser.parseBill(line));
		
		JavaPairRDD<MonthProductKey, Integer> monthProduct2SaleCount =
				allSales.reduceByKey((a, b) -> a + b);
		
		JavaPairRDD<String, Iterable<ProductReport>> month2ProductReports =
				monthProduct2SaleCount.mapToPair(saleCount -> {String[] monthProduct = saleCount._1().explodeKeys();
				                                               Integer count = saleCount._2();
				                                               return new Tuple2<>(monthProduct[0], new ProductReport(monthProduct[1], count));})
				                      .groupByKey();
		
		JavaPairRDD<String, Iterable<ProductReport>> month2BestFive =
				month2ProductReports.mapToPair(month -> {List<ProductReport> reportList = (List<ProductReport>) month._2();
				                                   List<ProductReport> best5 = ProductReports.takeBest(reportList, 5, new ReportComparator());
				                                   return new Tuple2<>(month._1(), best5);});
		
		month2BestFive.saveAsTextFile(outputPath);
		sc.close();
	}
}
