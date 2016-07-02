package it.uniroma3.radeon.sportlight.db;

import java.util.List;

import it.uniroma3.radeon.sportlight.data.Comment;

public interface CommentRepository {
	public void persistOne(Comment comment);
	public void persistMany(List<Comment> comments);
	public Comment findCommentById(String id);
}
