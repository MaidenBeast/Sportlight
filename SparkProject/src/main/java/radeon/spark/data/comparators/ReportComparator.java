package radeon.spark.data.comparators;

import java.util.Comparator;

import radeon.spark.data.ProductReport;

public class ReportComparator implements Comparator<ProductReport> {
	
	public int compare(ProductReport p1, ProductReport p2) {
		return p1.getValue().compareTo(p2.getValue());
	}
}
