package radeon.spark.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import radeon.spark.data.ProductReport;

public class ProductReports {
	
//	public static List<ProductReport> takeBest(List<ProductReport> list, int n, Comparator<ProductReport> comp) {
//		Collections.sort(list, Collections.reverseOrder(comp));
//		if (list.size() < n)
//			return list;
//		List<ProductReport> bestN = list.subList(0, n);
//		return bestN;
//	}
	
	public static List<ProductReport> takeBest(Iterable<ProductReport> iterable, int n, Comparator<ProductReport> comp) {
		List<ProductReport> list = new ArrayList<>();
		for (ProductReport pr : iterable) {
			list.add(pr);
		}
		Collections.sort(list, Collections.reverseOrder(comp));
		if (list.size() < n)
			return list;
		List<ProductReport> bestN = list.subList(0, n);
		return bestN;
	}
}
