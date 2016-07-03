package it.uniroma3.radeon.sportlight.db;

import java.util.List;

import it.uniroma3.radeon.sportlight.data.Post;

public interface PostRepository {
	public void persistOne(Post post);
	public void persistMany(List<Post> posts);
	public Post findPostById(String id, boolean alsoComments);
	public List<Post> findPostsByIds(List<String> ids, boolean alsoComments);
}
