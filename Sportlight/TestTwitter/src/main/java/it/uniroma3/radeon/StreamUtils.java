package it.uniroma3.radeon;

import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class StreamUtils {

	private static String CONSUMER_KEY = "Ljm2K7vc3SRea189FzhP0BlaZ";
	private static String CONSUMER_SECRET = "QyyMJLXSZjOS8uZoCrMBERn66OoB357TLW4eXjOUWRrfakvnk7";
	private static String ACCESS_TOKEN = "737245237077245952-GYwY6RL7aKDjnhUT5qzw2Nrxp5PDYHQ";
	private static String ACCESS_TOKEN_SECRET = "wxLBX9MWy6DaJ4PEk3SaHFPY6hnQKlmB93ZqehbEenyw5";

	public static OAuthAuthorization getAuth() {
		return new OAuthAuthorization(StreamUtils.getConfiguration());
	}

	public static Configuration getConfiguration() {
		return new ConfigurationBuilder().setOAuthConsumerKey(CONSUMER_KEY)
				.setOAuthConsumerSecret(CONSUMER_SECRET)
				.setOAuthAccessToken(ACCESS_TOKEN)
				.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
				.build();
	}

}
