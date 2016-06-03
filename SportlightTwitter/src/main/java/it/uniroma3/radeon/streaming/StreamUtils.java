package it.uniroma3.radeon.streaming;

import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

/**
 *  Class to authenticate with the Twitter streaming API.
 *
 *  Go to https://apps.twitter.com/
 *  Create your application and then get your own credentials (keys and access tokens tab)
 *
 *  See https://databricks-training.s3.amazonaws.com/realtime-processing-with-spark-streaming.html
 *  for help.
 *
 *  If you have the following error "error 401 Unauthorized":
 *  - it might be because of wrong credentials
 *  OR
 *  - a time zone issue (so be certain that the time zone on your computer is the good one)
 *
 */
public class StreamUtils {

  private static String CONSUMER_KEY = "Ljm2K7vc3SRea189FzhP0BlaZ";
  private static String CONSUMER_SECRET = "QyyMJLXSZjOS8uZoCrMBERn66OoB357TLW4eXjOUWRrfakvnk7";
  private static String ACCESS_TOKEN = "737245237077245952-GYwY6RL7aKDjnhUT5qzw2Nrxp5PDYHQ";
  private static String ACCESS_TOKEN_SECRET = "wxLBX9MWy6DaJ4PEk3SaHFPY6hnQKlmB93ZqehbEenyw5";

  public static OAuthAuthorization getAuth() {

    return new OAuthAuthorization(
        new ConfigurationBuilder().setOAuthConsumerKey(CONSUMER_KEY)
            .setOAuthConsumerSecret(CONSUMER_SECRET)
            .setOAuthAccessToken(ACCESS_TOKEN)
            .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
            .build());
  }
}
