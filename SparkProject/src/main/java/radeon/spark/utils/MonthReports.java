package radeon.spark.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import radeon.spark.data.MonthReport;

public class MonthReports {
	
	public static List<MonthReport> orderReports(Iterable<MonthReport> reports, Comparator<MonthReport> comp) {
		List<MonthReport> ordered = new ArrayList<>();
		for (MonthReport r : reports) {
			ordered.add(r);
		}
		Collections.sort(ordered, comp);
		return ordered;
	}

}
