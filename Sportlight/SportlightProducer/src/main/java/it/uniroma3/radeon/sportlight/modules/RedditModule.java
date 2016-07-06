package it.uniroma3.radeon.sportlight.modules;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

import it.uniroma3.radeon.sportlight.data.Comment;
import it.uniroma3.radeon.sportlight.data.Post;
import it.uniroma3.radeon.sportlight.db.CommentRepository;
import it.uniroma3.radeon.sportlight.db.MongoCommentRepository;
import it.uniroma3.radeon.sportlight.db.MongoPostRepository;
import it.uniroma3.radeon.sportlight.db.PostRepository;

public class RedditModule implements Module {
	private static final String REDDIT_URL_TEMPLATE = "https://www.reddit.com/r/Euro2016/.json?sort=new&raw_json=1";
	private static final String REDDIT_URL_POST_TEMPLATE = "https://www.reddit.com/r/Euro2016/comments/%s/new.json?sort=new&raw_json=1";
	private static final String REDDIT_URL_NEW_COMMENTS = "https://www.reddit.com/r/Euro2016/comments.json";

	private static final String MODIFIED_USER_AGENT = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:46.0) Gecko/20100101 Firefox/46.0";

	KafkaProducer<String, String> producer;

	PostRepository post_repo;
	CommentRepository comment_repo;

	public RedditModule() {
		try (InputStream props = Resources.getResource("producer.props").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
			post_repo = new MongoPostRepository();
			comment_repo = new MongoCommentRepository();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		this.bootstrap();
		this.listen();
	}

	private void bootstrap() {
		this.bootPosts();
		this.bootComments();
		this.producer.close();
	}

	private void listen() {

	}

	private void bootPosts() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.YEAR, -1); //prendo la data dell'anno precedente
		long prevYearTimeStamp = cal.getTime().getTime();

		boolean toIterate = true;

		String after_param = "";
		URL url = null;

		Map<String, Post> postMapTemp;

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

				postMapTemp = new HashMap<String, Post>(jsonChildren.size());

				for (JsonNode jsonChild : jsonChildren) { //per ogni post
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

					postMapTemp.put(post.getId(), post);

					System.out.println(mapper.writeValueAsString(post));
					//producer.send(new ProducerRecord<String, String>("sportlight",mapper.writeValueAsString(post)));

				}

				Set<String> fetchedPostIds = postMapTemp.keySet();
				Map<String, Post> mongoPosts = this.post_repo.findPostsByIds(fetchedPostIds, false);

				//differenza insiemistica tra gli id dei post scaricati ora da reddit e quelli già presenti su Mongo
				fetchedPostIds.removeAll(mongoPosts.keySet());

				List<Post> postsToPush = new ArrayList<Post>(fetchedPostIds.size());

				for (String postId : fetchedPostIds)
					postsToPush.add(postMapTemp.get(postId));

				//DEBUG
				System.out.println("ID dei nuovi commenti: "+fetchedPostIds);
				System.out.println("ID dei commenti già presenti su Mongo: "+mongoPosts.keySet());

				if (postsToPush.size() > 0) //se ci stanno dei nuovi commenti
					this.post_repo.persistMany(postsToPush); //salvo su Mongo tutti i post ancora non persistiti

			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (JsonParseException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			try {
				Thread.sleep(1000); //attendi per un secondo (per evitare eventuali blocchi)
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} while (toIterate);
	}

	private void bootComments() {

		//Leggo tutti i post 
		Map<String, Post> postList = this.post_repo.findAllPostsBySrcs(asList("reddit"), false);

		//per ogni post di Reddit presente effettua il bootstrap dei commenti
		for (Post post : postList.values()) {
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
				Map<String, Comment> commentsMap = this.visitCommentTree(jsonCommentRoot, post);

				//JsonNode jsonPostChildren = jsonPost.get("children");
				//JsonNode jsonPostChildrenData = jsonPostChildren.get(0).get("data");

				//String selftext = jsonPostChildrenData.get("selftext").asText();
				//post.setBody(selftext);

				/*if (!selftext.equals("")) {
					System.out.println("Selftext: "+selftext);
				}*/

				for (Comment comment : commentsMap.values()) { //per ogni commento
					System.out.println(mapper.writeValueAsString(comment)); //debug
				}

				if (commentsMap.size() > 0) { //sono presenti dei commenti
					Set<String> fetchedCommentsIds = commentsMap.keySet();
					Map<String, Comment> mongoComments = this.comment_repo.findCommentsByIds(fetchedCommentsIds);

					//differenza insiemistica tra gli id dei post scaricati ora da reddit e quelli già presenti su Mongo
					fetchedCommentsIds.removeAll(mongoComments.keySet());

					List<Comment> commentsToPush = new ArrayList<Comment>(fetchedCommentsIds.size());

					for (String commentId : fetchedCommentsIds)
						commentsToPush.add(commentsMap.get(commentId));

					//DEBUG
					System.out.println("ID dei nuovi commenti: "+fetchedCommentsIds);
					System.out.println("ID dei commenti già presenti su Mongo: "+mongoComments.keySet());

					if (commentsToPush.size()>0) //se ci stanno dei nuovi commenti
						this.comment_repo.persistMany(commentsToPush); //salvo su Mongo tutti i post ancora non persistiti
				} else {
					//DEBUG
					System.out.println("NON SONO PRESENTI COMMENTI");
				}

				//invio il post al topic "sportlight" di Kafka
				producer.send(new ProducerRecord<String, String>("sportlight", mapper.writeValueAsString(post)));

			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			try {
				Thread.sleep(1000); //attendi per un secondo (per evitare eventuali blocchi)
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private Map<String, Comment> visitCommentTree(JsonNode jsonComment, Post post) {
		Map<String, Comment> commentsMap = new HashMap<String, Comment>();

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

				commentsMap.put(comment.getId(), comment);
				//System.out.println("Comment: "+bodyComment);
			}

			JsonNode jsonCommentReplies = jsonCommentChildData.get("replies");

			if (jsonCommentReplies != null && jsonCommentReplies.isObject()) { //sono presenti delle risposte al commento
				Map<String, Comment> replies = visitCommentTree(jsonCommentReplies, post);
				commentsMap.putAll(replies);
			}
		}
		return commentsMap;
	}

}
