package radeon.utils;

import java.util.Comparator;

import radeon.data.MonthWritable;

public class MonthWritableComparator implements Comparator<MonthWritable> {
	
	public MonthWritableComparator() {}
	
	public int compare(MonthWritable m1, MonthWritable m2) {
		MonthComparator mcomp = new MonthComparator();
		return mcomp.compare(m1.getMonth().toString(), m2.getMonth().toString());
	}
}
