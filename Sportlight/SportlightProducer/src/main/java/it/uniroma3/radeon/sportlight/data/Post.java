package it.uniroma3.radeon.sportlight.data;

import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

@JsonIdentityInfo(generator=ObjectIdGenerators.PropertyGenerator.class, property="id")
public class Post {
	private String id;
	private String src;
	private String title;
	private String body;
	private List<Comment> comments;
	private List<String> topics;
	private String type;

	public Post() {
		this.title = "";
		this.body = "";
		this.comments = new LinkedList<Comment>();
		this.topics = new LinkedList<String>();
		this.type = "post";
	}

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
	
	public void addComment(Comment comment) {
		this.getComments().add(comment);
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "Post [id=" + id + ", src=" + src + ", title=" + title + ", body=" + body + ", comments=" + comments
				+ ", type=" + type + "]";
	}

	public List<String> getTopics() {
		return topics;
	}

	public void setTopics(List<String> topics) {
		this.topics = topics;
	}
	
	public void addTopic(String topic) {
		this.topics.add(topic);
	}
	
}
