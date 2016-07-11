package it.uniroma3.radeon.sportlight.db;

import java.util.List;
import java.util.Map;
import java.util.Set;

import it.uniroma3.radeon.sportlight.data.Post;

public interface PostRepository {
	public void persistOne(Post post);
	public void persistMany(List<Post> posts);
	public Post findPostById(String id, boolean alsoComments);
	public Map<String, Post> findPostsByIds(Set<String> ids, boolean alsoComments);
	public Map<String, Post> findAllPosts(boolean alsoComments);
	public Map<String, Post> findAllPostsBySrcs(List<String> srcs, boolean alsoComments);
	//public Map<String, Post> findAllPostsWithoutCommentsBySrcs(List<String> srcs);
}
