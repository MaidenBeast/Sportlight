package it.uniroma3.radeon.sportlight.db;

import static java.util.Arrays.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import it.uniroma3.radeon.sportlight.data.Comment;
import it.uniroma3.radeon.sportlight.data.Post;
import it.uniroma3.radeon.sportlight.db.mongo.MongoDataSource;

public class MongoPostRepositoryTest {
	private static MongoDataSource mongoDataSource;
	private static PostRepository post_repo;
	private static CommentRepository comment_repo;

	@BeforeClass
	public static void setUp() throws Exception {
		MongoPostRepositoryTest.post_repo = new MongoPostRepository();
		MongoPostRepositoryTest.comment_repo = new MongoCommentRepository();
		MongoPostRepositoryTest.mongoDataSource = new MongoDataSource();
		
		MongoPostRepositoryTest.mongoDataSource.getCollection("post").drop();
		
		List<Post> posts = new ArrayList<Post>(2);
		List<Comment> comments = new ArrayList<Comment>(4);

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
		
		Post post3 = new Post();
		post3.setId("post3");
		post3.setSrc("fb");
		post3.setTitle("post3");
		post3.setBody("post3");

		posts.add(post3);

		MongoPostRepositoryTest.post_repo.persistMany(posts);

		Comment comment1 = new Comment();
		comment1.setId("comment1");
		comment1.setBody("post1.comment1");
		comment1.setPost(post1);
		
		comments.add(comment1);

		//MongoRepositoryTest.comment_repo.persistOne(comment1);

		Comment comment2 = new Comment();
		comment2.setId("comment2");
		comment2.setBody("post1.comment2");
		comment2.setPost(post1);
		
		comments.add(comment2);

		//MongoRepositoryTest.comment_repo.persistOne(comment2);

		Comment comment3 = new Comment();
		comment3.setId("comment3");
		comment3.setBody("post2.comment3");
		comment3.setPost(post2);
		
		comments.add(comment3);

		//MongoRepositoryTest.comment_repo.persistOne(comment3);

		Comment comment4 = new Comment();
		comment4.setId("comment4");
		comment4.setBody("post2.comment4");
		comment4.setPost(post2);
		
		comments.add(comment4);

		//MongoRepositoryTest.comment_repo.persistOne(comment4);
		
		MongoPostRepositoryTest.comment_repo.persistMany(comments);
	}

	@Test
	public void testFindPostById() {
		Post post1 = this.post_repo.findPostById("post1", true);
		Post post2 = this.post_repo.findPostById("post2", true);
		Post post3 = this.post_repo.findPostById("post3", true);

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
		
		Assert.assertEquals("post3", post3.getId());
		Assert.assertEquals("post3", post3.getTitle());
		Assert.assertEquals("post3", post3.getBody());
		Assert.assertEquals("fb", post3.getSrc());

		Assert.assertEquals(post3.getComments().size(), 0);

		post1 = this.post_repo.findPostById("post1", false);
		post2 = this.post_repo.findPostById("post2", false);
		post3 = this.post_repo.findPostById("post3", false);

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
		
		Assert.assertEquals("post3", post3.getId());
		Assert.assertEquals("post3", post3.getTitle());
		Assert.assertEquals("post3", post3.getBody());
		Assert.assertEquals("fb", post3.getSrc());

		Assert.assertEquals(post3.getComments().size(), 0);
		
		Post nullPost = this.post_repo.findPostById("pincopallino", true);
		
		Assert.assertNull(nullPost);
		
	}

	@Test
	public void testFindCommentById() {
		Comment comment1 = this.comment_repo.findCommentById("comment1");
		Comment comment3 = this.comment_repo.findCommentById("comment3");

		Assert.assertEquals("comment1", comment1.getId());
		Assert.assertEquals("post1.comment1", comment1.getBody());

		Assert.assertEquals("comment3", comment3.getId());
		Assert.assertEquals("post2.comment3", comment3.getBody());
		
		Comment nullComment = this.comment_repo.findCommentById("nullComment");
		Assert.assertNull(nullComment);
	}
	
	@Test
	public void testFindCommentsByIds() {
		Map<String, Comment> comments = this.comment_repo.findCommentsByIds(new HashSet<String>(asList("comment1", "comment2", "comment4", "comment5")));
		
		Assert.assertEquals("comment1", comments.get("comment1").getId());
		Assert.assertEquals("post1.comment1", comments.get("comment1").getBody());
		
		Assert.assertEquals("comment2", comments.get("comment2").getId());
		Assert.assertEquals("post1.comment2", comments.get("comment2").getBody());
		
		Assert.assertEquals("comment4", comments.get("comment4").getId());
		Assert.assertEquals("post2.comment4", comments.get("comment4").getBody());
	}
	
	@Test
	public void testFindPostsByIds() {
		Map<String, Post> postMap1 = this.post_repo.findPostsByIds(new HashSet<String>(asList("post1", "post2")), false);
		Map<String, Post> postMap2 = this.post_repo.findPostsByIds(new HashSet<String>(asList("post1")), false);
		Map<String, Post> postMap3 = this.post_repo.findPostsByIds(new HashSet<String>(asList("post1", "post5")), false);
		
		Assert.assertEquals("post1", postMap1.get("post1").getId());
		Assert.assertEquals("post1", postMap1.get("post1").getTitle());
		Assert.assertEquals("post1", postMap1.get("post1").getBody());
		
		Assert.assertEquals("post2", postMap1.get("post2").getId());
		Assert.assertEquals("post2", postMap1.get("post2").getTitle());
		Assert.assertEquals("post2", postMap1.get("post2").getBody());
		
		Assert.assertTrue(postMap2.size() == 1);
		
		Assert.assertEquals("post1", postMap2.get("post1").getId());
		Assert.assertEquals("post1", postMap2.get("post1").getTitle());
		Assert.assertEquals("post1", postMap2.get("post1").getBody());
		
		Assert.assertTrue(postMap3.size() == 1);
		
		Assert.assertEquals("post1", postMap3.get("post1").getId());
		Assert.assertEquals("post1", postMap3.get("post1").getTitle());
		Assert.assertEquals("post1", postMap3.get("post1").getBody());
	}
	
	@Test
	public void testFindAllPosts() {
		Map<String, Post> posts = this.post_repo.findAllPosts(false);
		
		Assert.assertEquals("post1", posts.get("post1").getId());
		Assert.assertEquals("post1", posts.get("post1").getTitle());
		Assert.assertEquals("post1", posts.get("post1").getBody());
		
		Assert.assertEquals("post2", posts.get("post2").getId());
		Assert.assertEquals("post2", posts.get("post2").getTitle());
		Assert.assertEquals("post2", posts.get("post2").getBody());
		
		Assert.assertEquals("post3", posts.get("post3").getId());
		Assert.assertEquals("post3", posts.get("post3").getTitle());
		Assert.assertEquals("post3", posts.get("post3").getBody());
		
		Assert.assertTrue(posts.size() == 3);
	}
	
	@Test
	public void testFindAllPostsBySrcs() {
		Map<String, Post> posts = this.post_repo.findAllPostsBySrcs(asList("reddit"), false);
		
		Assert.assertEquals("post1", posts.get("post1").getId());
		Assert.assertEquals("post1", posts.get("post1").getTitle());
		Assert.assertEquals("post1", posts.get("post1").getBody());
		Assert.assertEquals("reddit", posts.get("post1").getSrc());
		
		Assert.assertEquals("post2", posts.get("post2").getId());
		Assert.assertEquals("post2", posts.get("post2").getTitle());
		Assert.assertEquals("post2", posts.get("post2").getBody());
		Assert.assertEquals("reddit", posts.get("post2").getSrc());
		
		Assert.assertTrue(posts.size() == 2);
	}

}
