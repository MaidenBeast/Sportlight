package radeon.spark.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import radeon.spark.data.Product;

public class Products {
	
	public static List<Product> takeBest(List<Product> list, int n, Comparator<Product> comp) {
		Collections.sort(list, Collections.reverseOrder(comp));
		if (list.size() < n)
			return list;
		List<Product> bestN = list.subList(0, n);
		return bestN;
	}

}
