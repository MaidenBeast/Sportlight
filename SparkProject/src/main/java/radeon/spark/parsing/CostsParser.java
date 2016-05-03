package radeon.spark.parsing;

import java.io.Serializable;

import scala.Tuple2;

public class CostsParser implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String delimiter = "=";
	
	public Tuple2<String, Integer> parseCosts(String line) {
		String[] prodCost = line.split(this.delimiter);
		String product = prodCost[0].trim();
		String cost = prodCost[1].trim();
		
		return new Tuple2<>(product, Integer.parseInt(cost));
	}
	
	public void setDelimiter(String newDel) {
		this.delimiter = newDel;
	}
}
