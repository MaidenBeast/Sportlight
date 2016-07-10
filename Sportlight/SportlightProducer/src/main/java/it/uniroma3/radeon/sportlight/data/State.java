package it.uniroma3.radeon.sportlight.data;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

@JsonIdentityInfo(generator=ObjectIdGenerators.PropertyGenerator.class, property="src")
public class State {
	private String src;
	private PostState post_state;
	private CommentState comment_state;
	
	public String getSrc() {
		return src;
	}
	
	public void setSrc(String src) {
		this.src = src;
	}
	
	public PostState getPost_state() {
		return post_state;
	}
	
	public void setPost_state(PostState post_state) {
		this.post_state = post_state;
	}
	
	public CommentState getComment_state() {
		return comment_state;
	}
	
	public void setComment_state(CommentState comment_state) {
		this.comment_state = comment_state;
	}
	
}
