package it.uniroma3.radeon.sa.data;

public abstract class DataBean {
	
	private String keyField;
	
	public DataBean(String keyField) {
		this.keyField = keyField;
	}
	
	public String getKeyField() {
		return this.keyField;
	}
}
