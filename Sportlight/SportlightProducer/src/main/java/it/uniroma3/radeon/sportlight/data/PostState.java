package it.uniroma3.radeon.sportlight.data;

public class PostState {
	private String newest_post_id;
	private String last_fetched_post_id;
	
	public String getNewest_post_id() {
		return newest_post_id;
	}
	
	public void setNewest_post_id(String newest_post_id) {
		this.newest_post_id = newest_post_id;
	}
	
	public String getLast_fetched_post_id() {
		return last_fetched_post_id;
	}
	
	public void setLast_fetched_post_id(String last_fetched_post_id) {
		this.last_fetched_post_id = last_fetched_post_id;
	}
	
}
