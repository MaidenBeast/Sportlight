package it.uniroma3.radeon.sportlight.db;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import it.uniroma3.radeon.sportlight.data.Comment;
import it.uniroma3.radeon.sportlight.data.Post;
import it.uniroma3.radeon.sportlight.db.mongo.MongoDataSource;

public class MongoCommentRepositoryTest {
	private static MongoDataSource mongoDataSource;
	private static PostRepository post_repo;
	private static CommentRepository comment_repo;

	@BeforeClass
	public static void setUp() throws Exception {
		MongoCommentRepositoryTest.post_repo = new MongoPostRepository();
		MongoCommentRepositoryTest.comment_repo = new MongoCommentRepository();
		MongoCommentRepositoryTest.mongoDataSource = new MongoDataSource();
		
		MongoCommentRepositoryTest.mongoDataSource.getCollection("post").drop();
		
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

		MongoCommentRepositoryTest.post_repo.persistMany(posts);

		Comment comment1 = new Comment();
		comment1.setId("comment1");
		comment1.setBody("post1.comment1");
		comment1.setPost(post1);

		MongoCommentRepositoryTest.comment_repo.persistOne(comment1);

		Comment comment2 = new Comment();
		comment2.setId("comment2");
		comment2.setBody("post1.comment2");
		comment2.setPost(post1);

		MongoCommentRepositoryTest.comment_repo.persistOne(comment2);

		Comment comment3 = new Comment();
		comment3.setId("comment3");
		comment3.setBody("post2.comment3");
		comment3.setPost(post2);

		MongoCommentRepositoryTest.comment_repo.persistOne(comment3);

		Comment comment4 = new Comment();
		comment4.setId("comment4");
		comment4.setBody("post2.comment4");
		comment4.setPost(post2);

		MongoCommentRepositoryTest.comment_repo.persistOne(comment4);
	}

	@Test
	public void testFindPostById() {
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

	@Test
	public void testFindCommentById() {
		Comment comment1 = this.comment_repo.findCommentById("comment1");
		Comment comment3 = this.comment_repo.findCommentById("comment3");

		Assert.assertEquals("comment1", comment1.getId());
		Assert.assertEquals("post1.comment1", comment1.getBody());

		Assert.assertEquals("comment3", comment3.getId());
		Assert.assertEquals("post2.comment3", comment3.getBody());
	}
	
	@Test
	public void testFindCommentsByIds() {
		List<Comment> comments = this.comment_repo.findCommentsByIds(asList("comment1", "comment2", "comment4", "comment5"));
		
		Assert.assertEquals("comment1", comments.get(0).getId());
		Assert.assertEquals("post1.comment1", comments.get(0).getBody());
		
		Assert.assertEquals("comment2", comments.get(1).getId());
		Assert.assertEquals("post1.comment2", comments.get(1).getBody());
		
		Assert.assertEquals("comment4", comments.get(2).getId());
		Assert.assertEquals("post2.comment4", comments.get(2).getBody());
	}

}
