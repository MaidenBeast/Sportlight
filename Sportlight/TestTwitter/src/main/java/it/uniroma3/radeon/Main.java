package it.uniroma3.radeon;

import java.util.List;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

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
						System.out.println("[id: "+String.valueOf(status_id)+
											", text: "+ status.getText()+
											", created_at: ]"+ status.getCreatedAt());
					if (status.getId() < last_id)
						last_id = status_id;

					if (status.getRetweetCount() > 0) {
						List<Status> retweets = twitter.getRetweets(status_id);

						for (Status retweet : retweets) { //retweets (da trattare come Comment)
							if (status.getLang().equals("en")) //prendo solo i retweet in lingua inglese
								System.out.println("[id: "+String.valueOf(retweet.getId())+
													", text: "+ retweet.getText()+
													", created_at ]"+ retweet.getCreatedAt());
						}
					}

				}
				query.setMaxId(last_id-1);
				i++; //giusto una prova
			} while (i<3);
		} catch (TwitterException e) {
			e.printStackTrace();
		}
	}

}
