package radeon.utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;

import radeon.data.ProductPairWritable;

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

}
