package it.uniroma3.radeon.sa.data;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.mllib.regression.LabeledPoint;

public class Post implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String id;
	private String src;
	private String title;
	private List<String> topics;
	private String body;
	private List<Comment> comments;
	private String type;
	
	public Post() {}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSrc() {
		return src;
	}

	public void setSrc(String src) {
		this.src = src;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<String> getTopics() {
		return topics;
	}

	public void setTopics(List<String> topics) {
		this.topics = topics;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public List<Comment> getComments() {
		return comments;
	}

	public void setComments(List<Comment> comments) {
		this.comments = comments;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String toString() {
		return "[" + this.id + "] " + "(" + this.src + ") "
	           + "Title: " + this.title + "\n"
	           + "Topics: " + this.topics.toString() + "\n"
	           + "Body: " + this.body + "\n"
	           + "Comments: " + "\n"
	           + this.comments.toString() + "\n";
	}
}
