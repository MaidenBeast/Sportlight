package radeon.spark.parsing;

import scala.Tuple2;

public class CostsParser {
	
	private String delimiter = "=";
	
	public Tuple2<String, Integer> parseCosts(String line) {
		String[] prodCost = line.split(this.delimiter);
		String product = prodCost[0];
		String cost = prodCost[1];
		
		return new Tuple2<>(product, Integer.parseInt(cost));
	}
	
	public void setDelimiter(String newDel) {
		this.delimiter = newDel;
	}
}
