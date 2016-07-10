package it.uniroma3.radeon;

import java.util.ArrayList;
import java.util.List;

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

public class Main {

	public static void main(String[] args) {
		Twitter twitter = new TwitterFactory(StreamUtils.getConfiguration()).getInstance();
		Query query = new Query("#Euro2016");
		query.setCount(100);

		int i = 0; //giusto una prova
		long last_id = Long.MAX_VALUE;

		try {
			do {
				QueryResult result = twitter.search(query);
				for (Status status : result.getTweets()) { //tweet principale (da trattare come Post)
					Long status_id = status.getId();

					if (status.getLang().equals("en")) //prendo solo i tweet in lingua inglese
						System.out.println("Tweet \"vecchio\" ==> [id: "+String.valueOf(status_id)+
								", text: "+ status.getText()+
								", created_at: ]"+ status.getCreatedAt());
					if (status.getId() < last_id)
						last_id = status_id;

					/*if (status.getRetweetCount() > 0) {
						List<Status> retweets = twitter.getRetweets(status_id);

						for (Status retweet : retweets) { //retweets (da trattare come Comment)
							if (status.getLang().equals("en")) //prendo solo i retweet in lingua inglese
								System.out.println("[id: "+String.valueOf(retweet.getId())+
										", text: "+ retweet.getText()+
										", created_at ]"+ retweet.getCreatedAt());
						}
					}*/

				}
				query.setMaxId(last_id-1);
				i++; //giusto una prova
			} while (i<3);

			TwitterStream twitterStream = new TwitterStreamFactory(StreamUtils.getConfiguration()).getInstance();

			StatusListener listener = new StatusListener() {

				public void onStatus(Status status) {
					Long status_id = status.getId();

					if (status.getLang().equals("en")) //prendo solo i tweet in lingua inglese
						System.out.println("Tweet \"nuovo\" ==> [id: "+String.valueOf(status_id)+
								", text: "+ status.getText()+
								", created_at: ]"+ status.getCreatedAt());
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

		} catch (TwitterException e) {
			e.printStackTrace();
		}
	}

}
