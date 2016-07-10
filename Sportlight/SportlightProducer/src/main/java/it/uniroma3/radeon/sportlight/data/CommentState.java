package it.uniroma3.radeon.sportlight.data;

public class CommentState {
	private String newest_scraped_post_id;
	private String last_scraped_post_id;
	
	public String getNewest_scraped_post_id() {
		return newest_scraped_post_id;
	}
	
	public void setNewest_scraped_post_id(String newest_scraped_post_id) {
		this.newest_scraped_post_id = newest_scraped_post_id;
	}
	
	public String getLast_scraped_post_id() {
		return last_scraped_post_id;
	}
	
	public void setLast_scraped_post_id(String last_scraped_post_id) {
		this.last_scraped_post_id = last_scraped_post_id;
	}
	
}
