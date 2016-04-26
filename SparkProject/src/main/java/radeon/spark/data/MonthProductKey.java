package radeon.spark.data;

public class MonthProductKey {
	
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
	
	public int hashCode() {
		return this.month.hashCode() + this.product.hashCode();
	}
	
	public boolean equals(Object o) {
		MonthProductKey other = null;
		try {
			other = (MonthProductKey) o;
		}
		catch (ClassCastException e) {
			return false;
		}
		return other.month.equals(this.month) && other.product.equals(this.product);
	}
}
