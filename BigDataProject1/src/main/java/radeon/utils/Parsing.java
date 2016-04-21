package radeon.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

public class Parsing {
	
	public static String getMonth(String date, String delim) {
		StringTokenizer st = new StringTokenizer(date, delim);
		String year = st.nextToken();
		String month = st.nextToken();
		
		return year + "-" + month;
	}
	
	public static List<String> splitProducts(StringTokenizer tokenizer) {
		List<String> products = new ArrayList<String>();
		while (tokenizer.hasMoreTokens()) {
			products.add(tokenizer.nextToken());
		}
		return products;
	}
	
	public static Set<String> splitDistinctProducts(StringTokenizer tokenizer) {
		Set<String> distProducts = new HashSet<>();
		List<String> allProducts = splitProducts(tokenizer);
		for (String s : allProducts) {
			distProducts.add(s);
		}
		
		return distProducts;
	}
	
	public static void skipFields(StringTokenizer tokenizer, int toSkip) {
		for(int i = 0; i < toSkip && tokenizer.hasMoreTokens(); i +=1) {
			tokenizer.nextToken();
		}
	}

}
