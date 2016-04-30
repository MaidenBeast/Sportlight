package radeon.utils;

import java.util.Comparator;

public class MonthComparator implements Comparator<String> {
	
	private String dateDelimiter = "-";
	
	public MonthComparator() {}
	
	public MonthComparator(String delim) {
		this.dateDelimiter = delim;
	}
	
	public int compare(String s1, String s2) {
		String[] yearMonth1 = s1.split(this.dateDelimiter);
		String[] yearMonth2 = s2.split(this.dateDelimiter);
		
		Integer month1 = Integer.parseInt(yearMonth1[1]);
		Integer month2 = Integer.parseInt(yearMonth2[1]);
		
		return month1.compareTo(month2);
	}

}
