package radeon.spark.data.comparators;

import java.util.Comparator;

import radeon.spark.data.Product;

public class CountComparator implements Comparator<Product> {
	
	public int compare(Product p1, Product p2) {
		return p1.getCount().compareTo(p2.getCount());
	}
}
