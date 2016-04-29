package radeon.spark.data;

import java.io.Serializable;

public class MonthReport implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String month;
	private Integer value;
	
	public MonthReport(String month, Integer value) {
		this.month = month;
		this.value = value;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}
	
	public String toString() {
		return this.month + ":" + this.value;
	}
}