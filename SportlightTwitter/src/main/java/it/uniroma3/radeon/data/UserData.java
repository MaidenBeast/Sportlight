package it.uniroma3.radeon.data;

import java.io.Serializable;

public class UserData implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String name;
	
	public UserData() {}
	
	public UserData(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
