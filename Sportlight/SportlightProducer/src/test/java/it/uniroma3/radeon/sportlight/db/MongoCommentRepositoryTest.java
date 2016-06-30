package it.uniroma3.radeon.sportlight.db;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import it.uniroma3.radeon.sportlight.data.Comment;
import it.uniroma3.radeon.sportlight.data.Post;

public class MongoCommentRepositoryTest {
	private PostRepository post_repo;
	private CommentRepository comment_repo;
	
	@Before
	public void setUp() throws Exception {
		this.post_repo = new MongoPostRepository();
		this.comment_repo = new MongoCommentRepository();
	}

	@Test
	public void test() {
		List<Post> posts = new ArrayList<Post>(2);
		
		Post post1 = new Post();
		post1.setId("post1");
		post1.setSrc("reddit");
		post1.setTitle("post1");
		post1.setBody("post1");
		
		posts.add(post1);
		
		Post post2 = new Post();
		post2.setId("post2");
		post2.setSrc("reddit");
		post2.setTitle("post2");
		post2.setBody("post2");
		
		posts.add(post2);
		
		this.post_repo.persistMany(posts);
		
		Comment comment1 = new Comment();
		comment1.setId("comment1");
		comment1.setBody("post1.comment1");
		comment1.setPost(post1);
		
		this.comment_repo.persistOne(comment1);
		
		Comment comment2 = new Comment();
		comment2.setId("comment2");
		comment2.setBody("post1.comment2");
		comment2.setPost(post1);
		
		this.comment_repo.persistOne(comment2);
		
		Comment comment3 = new Comment();
		comment3.setId("comment3");
		comment3.setBody("post2.comment3");
		comment3.setPost(post2);
		
		this.comment_repo.persistOne(comment3);
		
		Comment comment4 = new Comment();
		comment4.setId("comment4");
		comment4.setBody("post2.comment4");
		comment4.setPost(post2);
		
		this.comment_repo.persistOne(comment4);
		
	}

}
