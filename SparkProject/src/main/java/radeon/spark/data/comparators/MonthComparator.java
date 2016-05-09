package radeon.spark.data.comparators;

import java.util.Comparator;

import radeon.spark.data.MonthReport;
import radeon.spark.data.ProductReport;

public class MonthComparator implements Comparator<MonthReport> {
	
	private String delimiter = "-";
	
	public MonthComparator() {}
	
	public MonthComparator(String delim) {
		this.delimiter = delim;
	}
	public int compare(MonthReport m1, MonthReport m2) {
		String[] yearMonth1 = m1.getMonth().split(this.delimiter);
		String[] yearMonth2 = m2.getMonth().split(this.delimiter);
		
		Integer month1 = Integer.parseInt(yearMonth1[1]);
		Integer month2 = Integer.parseInt(yearMonth2[1]);
		
		return month1.compareTo(month2);
	}
}
