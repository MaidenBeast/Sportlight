package radeon.spark.data;

import java.io.Serializable;

public class ProductReport implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String product;
	private Integer value;
	
	public ProductReport(String product, Integer value) {
		this.product = product;
		this.value = value;
	}

	public String getProduct() {
		return product;
	}

	public void setProduct(String product) {
		this.product = product;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}
	
	public String toString() {
		return this.product + ":" + this.value;
	}

}
