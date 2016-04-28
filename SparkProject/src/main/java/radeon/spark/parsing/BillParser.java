package radeon.spark.parsing;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import radeon.spark.data.MonthProductKey;
import radeon.spark.data.ProductPair;
import scala.Tuple2;

public class BillParser {
	
	private String delimiter = ",";
	private String dateDelimiter = "-";
	
	public List<Tuple2<MonthProductKey, Integer>> parseBill(String line) {
		String[] dateAndProducts = line.split(this.delimiter);
		String month = this.getMonth(dateAndProducts[0], "-");
		
		List<Tuple2<MonthProductKey, Integer>> singleSales = new ArrayList<>();
		
		for (int i = 1; i < dateAndProducts.length; i+=1) {
			MonthProductKey mpk = new MonthProductKey(month, dateAndProducts[i]);
			Tuple2<MonthProductKey, Integer> tuple = new Tuple2<>(mpk, 1);
			singleSales.add(tuple);
		}
		
		return singleSales;	
	}
	
	public Set<Tuple2<String, Integer>> parseBillProductSales(String line) {
		List<Tuple2<MonthProductKey, Integer>> monthProductSales = this.parseBill(line);
		
		Set<Tuple2<String, Integer>> productSales = new HashSet<>();
		for (Tuple2<MonthProductKey, Integer> tuple : monthProductSales) {
			productSales.add(new Tuple2<>(tuple._1().getProduct(), 1));
		}
		
		return productSales;
	}
		
	public List<Tuple2<ProductPair, Integer>> parsePairs(String line) {
		
		String[] dateAndProducts = line.split(this.delimiter);
		
		Set<ProductPair> pairs = new HashSet<>();
		
		for (int i = 1; i < dateAndProducts.length; i +=1) {
			for (int j = 1; i < dateAndProducts.length; j += 1) {
				if (i != j) {
					String left = dateAndProducts[i];
					String right = dateAndProducts[j];
					pairs.add(new ProductPair(left, right));
				}
			}
		}
		
		List<Tuple2<ProductPair, Integer>> countedPairs = new ArrayList<>();
		for (ProductPair pair : pairs) {
			countedPairs.add(new Tuple2<>(pair, 1));
		}
		return countedPairs;
	}
	
	public List<MonthProductKey> parseBillKeysOnly(String line) {
		String[] dateAndProducts = line.split(this.delimiter);
		String month = this.getMonth(dateAndProducts[0], "-");
		
		List<MonthProductKey> saleKeys = new ArrayList<>();
		
		for (int i = 1; i < dateAndProducts.length; i += 1) {
			MonthProductKey mpk = new MonthProductKey(month, dateAndProducts[i]);
			saleKeys.add(mpk);
		}
		
		return saleKeys;
	}
	
	private String getMonth(String date, String sep) {
		String[] splitDate = date.split(this.dateDelimiter);
		String year = splitDate[0];
		String month = splitDate[1];
		
		return year + sep + month;
	}
	
	public void setDelimiter(String newDel) {
		this.delimiter = newDel;
	}
	
	public void setDateDelimiter(String newDel) {
		this.dateDelimiter = newDel;
	}
}
