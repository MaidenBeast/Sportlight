package it.uniroma3.radeon.sportlight.db;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import it.uniroma3.radeon.sportlight.data.CommentState;
import it.uniroma3.radeon.sportlight.data.PostState;
import it.uniroma3.radeon.sportlight.data.State;
import it.uniroma3.radeon.sportlight.db.mongo.MongoDataSource;
import it.uniroma3.radeon.sportlight.db.mongo.MongoStateRepository;

public class MongoStateRepositoryTest {
	private static StateRepository state_repo;
	private static MongoDataSource mongoDataSource;
	
	@BeforeClass
	public static void setUp() throws Exception {
		MongoStateRepositoryTest.mongoDataSource = new MongoDataSource();
		MongoStateRepositoryTest.state_repo = new MongoStateRepository();
		
		MongoStateRepositoryTest.mongoDataSource.getCollection("state").drop();
		
		State twitterState = new State();
		twitterState.setSrc("twitter");
		
		PostState twitterPostState = new PostState();
		twitterPostState.setLast_fetched_post_id("twitter_001");
		twitterPostState.setNewest_post_id("twitter_002");
		
		CommentState twitterCommentState = new CommentState();
		twitterCommentState.setLast_scraped_post_id("twitter_001");
		twitterCommentState.setNewest_scraped_post_id("twitter_002");
		
		twitterState.setPost_state(twitterPostState);
		twitterState.setComment_state(twitterCommentState);
		
		State redditState = new State();
		redditState.setSrc("reddit");
		
		PostState redditPostState = new PostState();
		redditPostState.setLast_fetched_post_id("reddit_001");
		redditPostState.setNewest_post_id("reddit_002");
		
		CommentState redditCommentState = new CommentState();
		redditCommentState.setLast_scraped_post_id("reddit_001");
		redditCommentState.setNewest_scraped_post_id("reddit_002");
		
		redditState.setPost_state(redditPostState);
		redditState.setComment_state(redditCommentState);
		
		state_repo.updateState(twitterState);
		state_repo.updateState(redditState);
	}
	
	@Test
	public void testGetStateBySrc() {
		State twitterState = this.state_repo.getStateBySrc("twitter");
		State redditState = this.state_repo.getStateBySrc("reddit");
		
		Assert.assertEquals("twitter", twitterState.getSrc());
		Assert.assertEquals("reddit", redditState.getSrc());
		
		Assert.assertEquals("twitter_001", twitterState.getPost_state().getLast_fetched_post_id());
		Assert.assertEquals("twitter_002", twitterState.getPost_state().getNewest_post_id());
		Assert.assertEquals("twitter_001", twitterState.getComment_state().getLast_scraped_post_id());
		Assert.assertEquals("twitter_002", twitterState.getComment_state().getNewest_scraped_post_id());
		
		Assert.assertEquals("reddit_001", redditState.getPost_state().getLast_fetched_post_id());
		Assert.assertEquals("reddit_002", redditState.getPost_state().getNewest_post_id());
		Assert.assertEquals("reddit_001", redditState.getComment_state().getLast_scraped_post_id());
		Assert.assertEquals("reddit_002", redditState.getComment_state().getNewest_scraped_post_id());
		
	}

}
