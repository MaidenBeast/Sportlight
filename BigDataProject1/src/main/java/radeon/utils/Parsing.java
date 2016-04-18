package radeon.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Parsing {
	
	public static String getMonth(String date, String delim) {
		StringTokenizer st = new StringTokenizer(date, delim);
		String year = st.nextToken();
		String month = st.nextToken();
		
		return year + "-" + month;
	}
	
	public static List<String> splitProducts(StringTokenizer tokenizer) {
		List<String> products = new ArrayList<>();
		while (tokenizer.hasMoreTokens()) {
			products.add(tokenizer.nextToken());
		}
		return products;
	}

}
