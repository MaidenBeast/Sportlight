package it.uniroma3.radeon.sportlight.modules;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/*
 * Forse si possono aumentare il numero di chiamate disponibili...
 * http://stackoverflow.com/questions/17172132/twitter-application-only-authentication-java-android-with-twitter4j
 */
public class TwitterModule extends Module {
	private static String CONSUMER_KEY = "Ljm2K7vc3SRea189FzhP0BlaZ";
	private static String CONSUMER_SECRET = "QyyMJLXSZjOS8uZoCrMBERn66OoB357TLW4eXjOUWRrfakvnk7";
	private static String ACCESS_TOKEN = "737245237077245952-GYwY6RL7aKDjnhUT5qzw2Nrxp5PDYHQ";
	private static String ACCESS_TOKEN_SECRET = "wxLBX9MWy6DaJ4PEk3SaHFPY6hnQKlmB93ZqehbEenyw5";
	
	private Twitter twitter;
	private Query query;
	
	private List<Long> toGetRetweetsIds;
	
	public TwitterModule() {
		super();
		this.twitter = new TwitterFactory(this.getConfiguration()).getInstance();
		this.query = new Query("#Euro2016");
		this.toGetRetweetsIds = new LinkedList<Long>();
	}
	
	@Override
	protected void bootstrap() {
		this.bootTweets();
		this.bootRetweets();
	}
	
	private void bootTweets() {
		this.query.setCount(100);
		long last_id = Long.MAX_VALUE;
		
		String euro2016StartDateString = "10/06/2016";
		DateFormat format = new SimpleDateFormat("dd/MM/yyyy", Locale.ITALY);
		
		boolean toIterate = true;

		try {
			Date euro2016StartDate = format.parse(euro2016StartDateString);
			
			do {
				QueryResult result = this.twitter.search(query);
				for (Status status : result.getTweets()) { //tweet principale (da trattare come Post)
					Long status_id = status.getId();
					Date createdAt = status.getCreatedAt();

					if (status.getLang().equals("en")) //prendo solo i tweet in lingua inglese
						System.out.println("[id: "+String.valueOf(status_id)+
											", text: "+ status.getText()+
											", created_at: "+ createdAt +"]");
					if (status_id < last_id)
						last_id = status_id;

					if (status.getRetweetCount() > 0) {
						this.toGetRetweetsIds.add(status_id);
					}
					
					if (createdAt.before(euro2016StartDate)) {
						toIterate = false;
						break;
					}

				}
				query.setMaxId(last_id-1);
				
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
		}
	}
	
	private void bootRetweets() {
		for (Long toGetRetweetsId : this.toGetRetweetsIds) {
			List<Status> retweets;
			try {
				retweets = twitter.getRetweets(toGetRetweetsId);
				for (Status retweet : retweets) { //retweets (da trattare come Comment)
					if (retweet.getLang().equals("en")) //prendo solo i retweet in lingua inglese
						System.out.println("[id: "+String.valueOf(retweet.getId())+
											", text: "+ retweet.getText()+
											", created_at: "+ retweet.getCreatedAt() +"]");
				}
				int remaining = this.twitter.getRateLimitStatus()
						.get("/statuses/retweets/:id")
						.getRemaining();
				
				int secondsUntilReset = this.twitter.getRateLimitStatus()
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

	@Override
	protected void listen() {
		// TODO Auto-generated method stub

	}
	
	private OAuthAuthorization getAuth() {
		return new OAuthAuthorization(this.getConfiguration());
	}

	private Configuration getConfiguration() {
		return new ConfigurationBuilder().setOAuthConsumerKey(CONSUMER_KEY)
				.setOAuthConsumerSecret(CONSUMER_SECRET)
				.setOAuthAccessToken(ACCESS_TOKEN)
				.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
				.build();
	}

}
