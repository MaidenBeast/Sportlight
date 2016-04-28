package radeon.spark.utils;

import radeon.spark.data.ProductPair;

public class ProductPairs {
	
	public static ProductPair calculateSupportAndConfidence(ProductPair pair, int billsPair, int totalBills, int billsProduct) {
		double totalBillsDiv = totalBills;
		double billsProductDiv = billsProduct;
		pair.setSupport(billsPair/totalBillsDiv);
		pair.setConfidence(billsPair/billsProductDiv);
		return pair;
	}
}
