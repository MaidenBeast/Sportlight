package it.uniroma3.radeon.sportlight.modules;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

import it.uniroma3.radeon.sportlight.data.Post;
import it.uniroma3.radeon.sportlight.data.State;
import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/*
 * Forse si possono aumentare il numero di chiamate disponibili...
 * http://stackoverflow.com/questions/17172132/twitter-application-only-authentication-java-android-with-twitter4j
 */
public class TwitterModule extends Module {
	private static final String CONSUMER_KEY = "Ljm2K7vc3SRea189FzhP0BlaZ";
	private static final String CONSUMER_SECRET = "QyyMJLXSZjOS8uZoCrMBERn66OoB357TLW4eXjOUWRrfakvnk7";
	private static final String ACCESS_TOKEN = "737245237077245952-GYwY6RL7aKDjnhUT5qzw2Nrxp5PDYHQ";
	private static final String ACCESS_TOKEN_SECRET = "wxLBX9MWy6DaJ4PEk3SaHFPY6hnQKlmB93ZqehbEenyw5";
	
	private Twitter twitter;
	private Query query;
	
	final private List<Post> toGetRetweetsPosts;
	
	private State twitterState;
	
	public TwitterModule() {
		super();
		this.twitter = new TwitterFactory(this.getConfiguration()).getInstance();
		this.query = new Query("#Euro2016");
		this.toGetRetweetsPosts = new LinkedList<Post>();
		this.twitterState = this.state_repo.getStateBySrc("twitter");
	}
	
	@Override
	protected void bootstrap() {
		this.bootTweets();
	}
	
	private void bootTweets() {
		this.query.setCount(100);
		long last_id = Long.MAX_VALUE;
		
		String euro2016StartDateString = "10/06/2016";
		DateFormat format = new SimpleDateFormat("dd/MM/yyyy", Locale.ITALY);
		
		boolean toIterate = true;
		
		Map<String, Post> postMapTemp;

		try {
			Date euro2016StartDate = format.parse(euro2016StartDateString);
			ObjectMapper mapper = new ObjectMapper();
			
			do {
				QueryResult result = this.twitter.search(query);
				List<Status> tweets = result.getTweets();
				
				postMapTemp = new HashMap<String, Post>(tweets.size());
				
				for (Status status : tweets) { //tweet principale (da trattare come Post)
					Long status_id = status.getId();
					Date createdAt = status.getCreatedAt();
					Post twitterPost = null;

					
					if (status.getLang().equals("en")) { //prendo solo i tweet in lingua inglese
						/*System.out.println("[id: "+String.valueOf(status_id)+
											", text: "+ status.getText()+
											", created_at: "+ createdAt +"]");*/
						twitterPost = new Post();
						twitterPost.setId("twitter_"+String.valueOf(status_id));
						twitterPost.setBody(status.getText());
						twitterPost.setSrc("twitter");
						
						postMapTemp.put(twitterPost.getId(), twitterPost);
						
						if (status.getRetweetCount() > 0) {
							this.toGetRetweetsPosts.add(twitterPost);
						}
						
						String jsonPost = mapper.writeValueAsString(twitterPost);
						
						//System.out.println(jsonPost); //DEBUG
						producer.send(new ProducerRecord<String, String>("sportlight", jsonPost));
						
					}
					
					if (status_id < last_id)
						last_id = status_id;
					
					if (createdAt.before(euro2016StartDate)) {
						toIterate = false;
						break;
					}

				}
				query.setMaxId(last_id-1);
				
				Set<String> fetchedPostIds = postMapTemp.keySet();
				Map<String, Post> mongoPosts = this.post_repo.findPostsByIds(fetchedPostIds, false);

				//differenza insiemistica tra gli id dei post scaricati ora da twitter e quelli gia' presenti su Mongo
				fetchedPostIds.removeAll(mongoPosts.keySet());

				List<Post> postsToPush = new ArrayList<Post>(fetchedPostIds.size());

				for (String postId : fetchedPostIds)
					postsToPush.add(postMapTemp.get(postId));

				//DEBUG
				System.out.println("ID dei nuovi post: "+fetchedPostIds);
				System.out.println("ID dei post gia' presenti su Mongo: "+mongoPosts.keySet());

				if (postsToPush.size() > 0) //se ci stanno dei nuovi post
					this.post_repo.persistMany(postsToPush); //salvo su Mongo tutti i post ancora non persistiti
				
				//System.out.print(this.twitter.getRateLimitStatus());
				
				int remaining = this.twitter.getRateLimitStatus()
						.get("/search/tweets")
						.getRemaining();
				
				int secondsUntilReset = this.twitter.getRateLimitStatus()
						.get("/search/tweets")
						.getSecondsUntilReset();
				
				int secondsToWait = secondsUntilReset/remaining;
				
				Thread.sleep(secondsToWait*1000);
				
			} while (toIterate);
		} catch (TwitterException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void listen() {
		final ObjectMapper mapper = new ObjectMapper();
		
		//intanto creo il thread per il bootstrap (contemporaneo) dei retweet
		Thread retweetThread = new Thread() {
			public void run() { //bootRetweets
				for (Post toGetRetweetsPost : toGetRetweetsPosts) {
					List<Status> retweets;
					try {
						Long toGetRetweetsId = Long.valueOf(toGetRetweetsPost.getId().lastIndexOf("_")+1);
						
						retweets = twitter.getRetweets(toGetRetweetsId);
						for (Status retweet : retweets) { //retweets (da trattare come Comment)
							if (retweet.getLang().equals("en")) //prendo solo i retweet in lingua inglese
								System.out.println("[id: "+String.valueOf(retweet.getId())+
													", text: "+ retweet.getText()+
													", created_at: "+ retweet.getCreatedAt() +"]");
						}
						int remaining = twitter.getRateLimitStatus()
								.get("/statuses/retweets/:id")
								.getRemaining();
						
						int secondsUntilReset = twitter.getRateLimitStatus()
								.get("/statuses/retweets/:id")
								.getSecondsUntilReset();
						
						int secondsToWait = secondsUntilReset/remaining;
						
						Thread.sleep(secondsToWait*1000);
					} catch (TwitterException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		retweetThread.start();

		//e intanto vado a prendere pure i nuovi tweet
		TwitterStream twitterStream = new TwitterStreamFactory(this.getConfiguration()).getInstance();
		
		final List<Post> postBuffer = new ArrayList<Post>(50);
		
		Properties properties = null;
		
		try (InputStream props = Resources.getResource("producer.props").openStream()) {
			properties = new Properties();
			properties.load(props);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		final KafkaProducer<String, String> statusProducer = new KafkaProducer<>(properties);
		
		StatusListener listener = new StatusListener() {
			
			public void onStatus(Status status) {
				Long status_id = status.getId();
				Post twitterPost = null;

				if (status.getLang().equals("en")) { //prendo solo i tweet in lingua inglese
					/*System.out.println("Tweet \"nuovo\" ==> [id: "+String.valueOf(status_id)+
							", text: "+ status.getText()+
							", created_at: ]"+ status.getCreatedAt());*/
					
					twitterPost = new Post();
					twitterPost.setId("twitter_"+String.valueOf(status_id));
					twitterPost.setBody(status.getText());
					twitterPost.setSrc("twitter");
					
					postBuffer.add(twitterPost);
					
					String jsonPost = null;
					try {
						jsonPost = mapper.writeValueAsString(twitterPost);
						//System.out.println(jsonPost); //DEBUG
						statusProducer.send(new ProducerRecord<String, String>("sportlight", jsonPost));
					} catch (JsonProcessingException e) {
						e.printStackTrace();
					}
					
					if (postBuffer.size() >= 50) { //il buffer e' uguale o superiore a 50
						post_repo.persistMany(postBuffer);
						postBuffer.clear();
					}
					
				}
			}

			public void onTrackLimitationNotice(int arg0) {}

			public void onStallWarning(StallWarning arg0) {}

			public void onScrubGeo(long arg0, long arg1) {}

			public void onDeletionNotice(StatusDeletionNotice arg0) {}
			
			public void onException(Exception ex) {
				ex.printStackTrace();
			}

		};
		
		List<String> queries = new ArrayList<String>();
		queries.add(query.getQuery());
		
		twitterStream.addListener(listener);
		
		String[] trackQueries = (String[]) queries.toArray(new String[queries.size()]);

		FilterQuery filterQuery = new FilterQuery();
		twitterStream.filter(filterQuery.track(trackQueries));
		
	}
	
	/*private OAuthAuthorization getAuth() {
		return new OAuthAuthorization(this.getConfiguration());
	}*/

	private Configuration getConfiguration() {
		return new ConfigurationBuilder().setOAuthConsumerKey(CONSUMER_KEY)
				.setOAuthConsumerSecret(CONSUMER_SECRET)
				.setOAuthAccessToken(ACCESS_TOKEN)
				.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
				.build();
	}

}
