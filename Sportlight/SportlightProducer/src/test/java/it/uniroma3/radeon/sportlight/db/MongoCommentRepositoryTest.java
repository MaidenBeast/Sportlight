package it.uniroma3.radeon.sportlight.db;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
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
	public void testPersist() {
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
	
	@Test
	public void testFindById() {
		Post post1 = this.post_repo.findByPostId("post1", true);
		Post post2 = this.post_repo.findByPostId("post2", true);
		
		Assert.assertEquals("post1", post1.getId());
		Assert.assertEquals("post1", post1.getTitle());
		Assert.assertEquals("post1", post1.getBody());
		Assert.assertEquals("reddit", post1.getSrc());
		
		Assert.assertNotEquals(post1.getComments().size(), 0);
		
		Assert.assertEquals("post2", post2.getId());
		Assert.assertEquals("post2", post2.getTitle());
		Assert.assertEquals("post2", post2.getBody());
		Assert.assertEquals("reddit", post2.getSrc());
		
		Assert.assertNotEquals(post2.getComments().size(), 0);
		
		post1 = this.post_repo.findByPostId("post1", false);
		post2 = this.post_repo.findByPostId("post2", false);
		
		Assert.assertEquals("post1", post1.getId());
		Assert.assertEquals("post1", post1.getTitle());
		Assert.assertEquals("post1", post1.getBody());
		Assert.assertEquals("reddit", post1.getSrc());
		
		Assert.assertEquals(post1.getComments().size(), 0);
		
		Assert.assertEquals("post2", post2.getId());
		Assert.assertEquals("post2", post2.getTitle());
		Assert.assertEquals("post2", post2.getBody());
		Assert.assertEquals("reddit", post2.getSrc());
		
		Assert.assertEquals(post2.getComments().size(), 0);
	}

}
