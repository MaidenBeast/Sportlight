package radeon.spark.parsing;

import java.util.ArrayList;
import java.util.List;

import radeon.spark.data.MonthProductKey;
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
