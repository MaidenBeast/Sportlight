package it.uniroma3.radeon.sportlight.db;

import java.util.List;
import java.util.Map;
import java.util.Set;

import it.uniroma3.radeon.sportlight.data.Comment;

public interface CommentRepository {
	public void persistOne(Comment comment);
	public void persistMany(List<Comment> comments);
	public Comment findCommentById(String id);
	public Map<String, Comment> findCommentsByIds(Set<String> ids);
}
