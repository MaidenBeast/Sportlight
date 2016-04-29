package radeon.spark.data;

import java.io.Serializable;

import scala.Tuple2;

public class MonthProductKey implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String month;
	private String product;
	
	public MonthProductKey(String month, String product) {
		this.month = month;
		this.product = product;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getProduct() {
		return product;
	}

	public void setProduct(String product) {
		this.product = product;
	}
	
	public String[] explodeKeys() {
		String[] keys = {this.month, this.product};
		return keys;
	}
	
	public Tuple2<String, MonthProductKey> toTupleKeyMonth() {
		return new Tuple2<>(this.month, this);
	}
	
	public Tuple2<String, MonthProductKey> toTupleKeyProduct() {
		return new Tuple2<>(this.product, this);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((month == null) ? 0 : month.hashCode());
		result = prime * result + ((product == null) ? 0 : product.hashCode());
		return result;
	}
		
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MonthProductKey other = (MonthProductKey) obj;
		if (month == null) {
			if (other.month != null)
				return false;
		} else if (!month.equals(other.month))
			return false;
		if (product == null) {
			if (other.product != null)
				return false;
		} else if (!product.equals(other.product))
			return false;
		return true;
	}
	
	public String toString() {
		return this.month + "," + this.product;
	}
}
