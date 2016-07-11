package it.uniroma3.radeon.sa.functions.mappers;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.uniroma3.radeon.sa.data.Post;

public class PostMapper extends JSONMapper<Post> {
	
	private static final long serialVersionUID = 1L;

	public PostMapper() {
		super(Post.class);
	}

	@Override
	public Post call(String json) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Post post = mapper.readValue(json, Post.class);
		return post;
	}
}
