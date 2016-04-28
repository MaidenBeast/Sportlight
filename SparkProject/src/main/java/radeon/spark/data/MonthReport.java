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
	
	

}
