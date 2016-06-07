package it.uniroma3.radeon.sportlight;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestReddit {
	private static final String REDDIT_URL_TEMPLATE = "https://www.reddit.com/r/Euro2016/.json?sort=new&raw_json=1";
	private static final String REDDIT_URL_POST_TEMPLATE = "https://www.reddit.com/r/Euro2016/comments/%s/new.json?sort=new&raw_json=1";
	
	private static final String MODIFIED_USER_AGENT = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:46.0) Gecko/20100101 Firefox/46.0";
	
	private List<String> postIds; //in futuro questa lista verrà letta da MongoDB
	
	public void bootstrap() {
		this.bootPosts();
		this.bootComments();
		this.postIds = new LinkedList<String>();
	}
	
	private void bootPosts() {
		Calendar cal = Calendar.getInstance();
		Date today = cal.getTime();
		cal.add(Calendar.YEAR, -1); //prendo la data dell'anno precedente
		long prevYearTimeStamp = cal.getTime().getTime();
		
		boolean toIterate = true;
		
		String after_param = "";
		URL url = null;
		do {
			try {
				url = new URL(REDDIT_URL_TEMPLATE.concat(after_param));
				URLConnection conn = url.openConnection();
				
				//workaround per l'errore HTTP 429 (Too Many Requests)
				conn.setRequestProperty("User-Agent", MODIFIED_USER_AGENT);
				
				ObjectMapper mapper = new ObjectMapper();
				
				System.out.println("\nGetting Reddit data from "+url.toURI());
				
				JsonNode jsonRoot = mapper.readTree(conn.getInputStream());
				JsonNode jsonData = jsonRoot.get("data");
				JsonNode jsonChildren = jsonData.get("children");
				
				after_param = "&after=".concat(jsonData.get("after").asText());
				
				for (JsonNode jsonChild : jsonChildren) { 
					JsonNode jsonChildData = jsonChild.get("data");
					this.postIds.add(jsonChildData.get("id").asText());
					long createTime = jsonChildData.get("created").asLong()*1000;
					
					//se il post è stato pubblicato un'anno fa, allora blocca entrambi i cicli
					if (createTime < prevYearTimeStamp) {
						toIterate = false;
						break;
					}
					System.out.println(jsonChildData);
				}
				
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(1000); //attendi per un secondo (per evitare eventuali blocchi)
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} while (toIterate);
	}
	
	private void bootComments() {
		for (String postId : this.postIds) {
			URL url = null;
			String postUrl = String.format(REDDIT_URL_POST_TEMPLATE, postId);
			
			try {
				url = new URL(postUrl);
				URLConnection conn = url.openConnection();
				
				//workaround per l'errore HTTP 429 (Too Many Requests)
				conn.setRequestProperty("User-Agent", MODIFIED_USER_AGENT);
				
				ObjectMapper mapper = new ObjectMapper();
				
				System.out.println("\nGetting Reddit data from "+url.toURI());
				
				JsonNode jsonRoot = mapper.readTree(conn.getInputStream());
				
				JsonNode jsonPost = jsonRoot.get(0).get("data");
				JsonNode jsonComment = jsonRoot.get(1).get("data");
				
				JsonNode jsonPostChildren = jsonPost.get("children");
				JsonNode jsonPostChildrenData = jsonPost.get(0).get("data");
				
				String selftext = jsonPostChildrenData.get("selftext").asText();
				
				if (!selftext.equals("")) {
					System.out.println("Selftext: "+selftext);
				}
				
				JsonNode jsonCommentChildren = jsonPost.get("children");
				
				for (JsonNode jsonCommentChild : jsonCommentChildren) {
					JsonNode jsonCommentChildData = jsonCommentChild.get("data");
					String bodyComment = jsonCommentChildData.get("body").asText();
					System.out.println("Comment :"+bodyComment);
				}
				
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(1000); //attendi per un secondo (per evitare eventuali blocchi)
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
