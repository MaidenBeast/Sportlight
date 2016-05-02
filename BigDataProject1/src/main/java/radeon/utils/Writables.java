package radeon.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;

import radeon.data.MonthWritable;
import radeon.data.ProductPairWritable;
import radeon.data.ProductWritable;

public class Writables {
		
	public static Set<ProductPairWritable> generatePairs(Collection<String> products) {
		Set<ProductPairWritable> pairs = new HashSet<ProductPairWritable>();
		
		if (products.size()==1) { // c'è solo un elemento nell'insieme
			return pairs;
		}
		
		for (String p1 : products) {
			for (String p2 : products) {
				if (!p1.equals(p2)) {
					ProductPairWritable newPair = new ProductPairWritable(new Text(p1), new Text(p2));
					pairs.add(newPair);
				}
			}
		}
		return pairs;
	}
	
	public static List<ProductWritable> takeBest(Iterable<ProductWritable> products, int n) {
		List<ProductWritable> prodList = new ArrayList<>();
		for (ProductWritable prod : products) {
			prodList.add(prod);
		}
		Collections.sort(prodList, Collections.reverseOrder());
		if (prodList.size() < n)
			return prodList;
		return prodList.subList(0, n);
	}
	
	public static List<MonthWritable> makeMonthList(Iterable<MonthWritable> months) {
		List<MonthWritable> monthList = new ArrayList();
		for (MonthWritable month : months) {
			monthList.add(month);
		}
		Collections.sort(monthList, new MonthWritableComparator());
		return monthList;
	}

}
