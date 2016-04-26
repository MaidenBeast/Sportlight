package radeon.spark.data;

import java.io.Serializable;

public class Product implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String name;
	private Integer count;
	private Integer revenue;
	
	public Product(String name, Integer count, Integer revenue) {
		this.name = name;
		this.count = count;
		this.revenue = revenue;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public Integer getRevenue() {
		return revenue;
	}

	public void setRevenue(Integer revenue) {
		this.revenue = revenue;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.name);
		if (this.count != 0) {
			sb.append(" " +this.count);
		}
		if (this.revenue != 0) {
			sb.append(" " +this.revenue);
		}
		return sb.toString();
	}
	
	
}
