package it.uniroma3.radeon.sportlight;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

import it.uniroma3.radeon.sportlight.data.Comment;
import it.uniroma3.radeon.sportlight.data.Post;
import it.uniroma3.radeon.sportlight.db.MongoPostRepository;
import it.uniroma3.radeon.sportlight.db.PostRepository;

public class TestReddit {
	private static final String REDDIT_URL_TEMPLATE = "https://www.reddit.com/r/Euro2016/.json?sort=new&raw_json=1";
	private static final String REDDIT_URL_POST_TEMPLATE = "https://www.reddit.com/r/Euro2016/comments/%s/new.json?sort=new&raw_json=1";
	
	private static final String MODIFIED_USER_AGENT = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:46.0) Gecko/20100101 Firefox/46.0";
	
	private List<Post> postList; //in futuro questa lista verrà letta da MongoDB
	
	KafkaProducer<String, String> producer;
	
	PostRepository post_repo;
	
	public TestReddit() {
		this.postList = new LinkedList<Post>();
		try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
            post_repo = new MongoPostRepository();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void bootstrap() {
		this.bootPosts();
		this.bootComments();
		this.producer.close();
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
					
					long createTime = jsonChildData.get("created").asLong()*1000;
					
					//se il post è stato pubblicato un'anno fa, allora blocca entrambi i cicli
					if (createTime < prevYearTimeStamp) {
						toIterate = false;
						break;
					}
					
					Post post = new Post();
					post.setId("reddit_"+
									jsonChildData.get("name").asText());
					post.setBody(jsonChildData.get("selftext").asText());
					post.setTitle(jsonChildData.get("title").asText());
					post.setSrc("reddit");
					
					this.postList.add(post);
					
					System.out.println(mapper.writeValueAsString(post));
					//producer.send(new ProducerRecord<String, String>("sportlight",mapper.writeValueAsString(post)));
					
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
		//for (String postId : this.postList) {
		for (Post post : this.postList) {
			URL url = null;
			String postId = post.getId();
			postId = postId.substring(postId.lastIndexOf("_")+1);
			String postUrl = String.format(REDDIT_URL_POST_TEMPLATE, postId);
			ObjectMapper mapper = new ObjectMapper();
			
			try {
				url = new URL(postUrl);
				URLConnection conn = url.openConnection();
				
				//workaround per l'errore HTTP 429 (Too Many Requests)
				conn.setRequestProperty("User-Agent", MODIFIED_USER_AGENT);
				
				System.out.println("\nGetting Reddit data from "+url.toURI());
				
				JsonNode jsonRoot = mapper.readTree(conn.getInputStream());
				
				//JsonNode jsonPost = jsonRoot.get(0).get("data");
				JsonNode jsonCommentRoot = jsonRoot.get(1);
				List<Comment> comments = this.visitCommentTree(jsonCommentRoot, post);
				
				//JsonNode jsonPostChildren = jsonPost.get("children");
				//JsonNode jsonPostChildrenData = jsonPostChildren.get(0).get("data");
				
				//String selftext = jsonPostChildrenData.get("selftext").asText();
				//post.setBody(selftext);
				
				/*if (!selftext.equals("")) {
					System.out.println("Selftext: "+selftext);
				}*/
				
				for (Comment comment : comments) {
					System.out.println(mapper.writeValueAsString(comment));
					//producer.send(new ProducerRecord<String, String>("sportlight", mapper.writeValueAsString(comment)));
				}
				//persisto il post su MongoDB
				post_repo.persistOne(post);
				
				//invio al topic Kafka
				producer.send(new ProducerRecord<String, String>("sportlight", mapper.writeValueAsString(post)));
				
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
	
	private List<Comment> visitCommentTree(JsonNode jsonComment, Post post) {
		List<Comment> comments = new LinkedList<Comment>();
		
		JsonNode jsonCommentChildren = jsonComment.get("data").get("children");
		
		for (JsonNode jsonCommentChild : jsonCommentChildren) {
			
			JsonNode jsonCommentChildData = jsonCommentChild.get("data");
			JsonNode jsonCommentChildDataBody = jsonCommentChildData.get("body");
			
			if (jsonCommentChildDataBody != null) {
				
				Comment comment = new Comment();
				comment.setId(jsonCommentChildData.get("name").asText());
				String bodyComment = jsonCommentChildData.get("body").asText();
				comment.setBody(bodyComment);
				comment.setPost(post);
				
				post.addComment(comment);
				
				comments.add(comment);
				//System.out.println("Comment: "+bodyComment);
			}
			
			JsonNode jsonCommentReplies = jsonCommentChildData.get("replies");
			
			if (jsonCommentReplies != null && jsonCommentReplies.isObject()) { //sono presenti delle risposte al commento
				List<Comment> replies = visitCommentTree(jsonCommentReplies, post);
				comments.addAll(replies);
			}
		}
		return comments;
	}
}
