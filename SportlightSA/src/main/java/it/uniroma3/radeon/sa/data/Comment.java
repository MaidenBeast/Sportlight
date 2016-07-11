package it.uniroma3.radeon.sa.data;

import java.io.Serializable;

public class Comment implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String id;
	private String body;
	private String type;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	
	public String toString() {
		return "[" + this.id + "]" + "\n"
	           + "Body: " + this.body + "\n"
	           + "Type: " + this.type + "\n";
	}
}
