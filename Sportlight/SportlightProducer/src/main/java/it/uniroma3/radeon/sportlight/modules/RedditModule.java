package it.uniroma3.radeon.sportlight.modules;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.uniroma3.radeon.sportlight.data.Comment;
import it.uniroma3.radeon.sportlight.data.Post;
import it.uniroma3.radeon.sportlight.data.State;

public class RedditModule extends Module {
	private static final String REDDIT_URL_TEMPLATE = "https://www.reddit.com/r/Euro2016/.json?sort=new&raw_json=1";
	private static final String REDDIT_URL_POST_TEMPLATE = "https://www.reddit.com/r/Euro2016/comments/%s/new.json?sort=new&raw_json=1";
	private static final String REDDIT_URL_NEW_COMMENTS = "https://www.reddit.com/r/Euro2016/comments.json";

	private static final String MODIFIED_USER_AGENT = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:46.0) Gecko/20100101 Firefox/46.0";
	
	private State redditState;

	public RedditModule() {
		super();
		this.redditState = this.state_repo.getStateBySrc("reddit");
	}
	
	@Override
	protected void bootstrap() {
		this.bootPosts();
		this.bootComments();
	}
	
	@Override
	protected void listen() {
		URL urlComments = null;
		ObjectMapper mapper = new ObjectMapper();
		Map<String, JsonNode> newCommentsMap = new HashMap<String, JsonNode>();

		while (true) { //loop infinito (per lo streaming
			try {
				urlComments = new URL(REDDIT_URL_NEW_COMMENTS);
				URLConnection connComments = urlComments.openConnection();

				//workaround per l'errore HTTP 429 (Too Many Requests)
				connComments.setRequestProperty("User-Agent", MODIFIED_USER_AGENT);

				System.out.println("\nGetting Reddit data from "+urlComments.toURI());

				JsonNode jsonRoot = mapper.readTree(connComments.getInputStream());
				JsonNode jsonData = jsonRoot.get("data");
				JsonNode jsonChildren = jsonData.get("children");
				
				newCommentsMap.clear(); ///svuota la mappa
				
				//itera per leggere tutti i commenti
				for (JsonNode jsonChild : jsonChildren) {
					JsonNode jsonChildData = jsonChild.get("data");
					String commentId = "reddit_"+jsonChildData.get("name").asText();
					//ogno commento lo mappo per id commento
					newCommentsMap.put(commentId, jsonChildData);
				}
				
				Set<String> newCommentsIds = newCommentsMap.keySet();
				//intanto verifico quali sono i commenti presenti gia' su MongoDB
				Map<String, Comment> mongoComments = this.comment_repo.findCommentsByIds(newCommentsIds);
				
				//differenza insiemistica tra gli id dei nuovi commenti scaricati ora da reddit e quelli gia' presenti su Mongo
				newCommentsIds.removeAll(mongoComments.keySet());
				
				List<Comment> commentsToPush = new ArrayList<Comment>(newCommentsIds.size());

				for (String commentId : newCommentsIds) {
					JsonNode commentNode = newCommentsMap.get(commentId);
					Comment newComment = new Comment();
					
					newComment.setId("reddit_"+commentNode.get("name").asText());
					String bodyComment = commentNode.get("body").asText();
					newComment.setBody(bodyComment);
					
					//per controlla se pure il post e' gia' presente su Mongo...
					String postId = commentNode.get("link_id").asText();
					Post post = post_repo.findPostById("reddit_"+postId, false);

					/*
					 * se il commento fa riferimento a un nuovo post:
					 * 	- effettua il fetch da reddit del post
					 * 	- persistilo (assieme al nuovo comment) su Mongo
					 */
					if (post == null) {
						Thread.sleep(1000); //intanto meglio aspettare un altro secondo
						String postIdSub = postId.substring(postId.lastIndexOf("_")+1);
						String postUrl = String.format(REDDIT_URL_POST_TEMPLATE, postIdSub);

						URL urlPost = new URL(postUrl);
						URLConnection connPost = urlPost.openConnection();

						//workaround per l'errore HTTP 429 (Too Many Requests)
						connPost.setRequestProperty("User-Agent", MODIFIED_USER_AGENT);

						System.out.println("\nGetting Reddit data from "+urlPost.toURI());

						JsonNode jsonRootPost = mapper.readTree(connPost.getInputStream());
						JsonNode jsonPost = jsonRootPost.get(0).get("data");

						JsonNode jsonPostChildren = jsonPost.get("children");
						JsonNode jsonPostChildrenData = jsonPostChildren.get(0).get("data");

						String selftext = jsonPostChildrenData.get("selftext").asText();
						String title = jsonPostChildrenData.get("title").asText();

						post = new Post();
						post.setId("reddit_"+postId);
						post.setSrc("reddit");
						post.setTitle(title);
						post.setBody(selftext);
						post.addComment(newComment);
						
						//System.out.println(mapper.writeValueAsString(post));
						
						this.post_repo.persistOne(post);
						
					} else { //altrimenti aggiungi il commento tra quelli da persistere tutt'assieme su Mongo
						newComment.setPost(post);
						commentsToPush.add(newComment);
					}
					//System.out.println(mapper.writeValueAsString(newComment));
					//invio il commento al topic "sportlight" di Kafka
					producer.send(new ProducerRecord<String, String>("sportlight", mapper.writeValueAsString(newComment)));
				}
				
				//DEBUG
				System.out.println("ID dei nuovi commenti: "+newCommentsIds);
				System.out.println("ID dei commenti gia' presenti su Mongo: "+mongoComments.keySet());
				
				if (commentsToPush.size() > 0) //se ci stanno dei commenti da persistere
					this.comment_repo.persistMany(commentsToPush); //salvo su Mongo tutti i commenti ancora non persistiti
				
				Thread.sleep(1000); //ascolta ogni secondo
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
	}

	protected void bootPosts() {
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

					//se il post e' stato pubblicato un'anno fa, allora blocca il ciclo esterno
					if (createTime < prevYearTimeStamp)
						toIterate = false;

					Post post = new Post();
					post.setId("reddit_"+
							jsonChildData.get("name").asText());
					post.setBody(jsonChildData.get("selftext").asText());
					post.setTitle(jsonChildData.get("title").asText());
					post.setSrc("reddit");

					postMapTemp.put(post.getId(), post);

					String jsonPost = mapper.writeValueAsString(post);
					
					//System.out.println(jsonPost); //DEBUG
					producer.send(new ProducerRecord<String, String>("sportlight",jsonPost));

				}

				Set<String> fetchedPostIds = postMapTemp.keySet();
				Map<String, Post> mongoPosts = this.post_repo.findPostsByIds(fetchedPostIds, false);

				//differenza insiemistica tra gli id dei post scaricati ora da reddit e quelli gia' presenti su Mongo
				fetchedPostIds.removeAll(mongoPosts.keySet());

				List<Post> postsToPush = new ArrayList<Post>(fetchedPostIds.size());

				for (String postId : fetchedPostIds)
					postsToPush.add(postMapTemp.get(postId));

				//DEBUG
				System.out.println("ID dei nuovi post: "+fetchedPostIds);
				System.out.println("ID dei post gia' presenti su Mongo: "+mongoPosts.keySet());

				if (postsToPush.size() > 0) //se ci stanno dei nuovi post
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

				/*for (Comment comment : commentsMap.values()) { //per ogni commento
					System.out.println(mapper.writeValueAsString(comment)); //debug
				}*/

				if (commentsMap.size() > 0) { //sono presenti dei commenti
					Set<String> fetchedCommentsIds = commentsMap.keySet();
					Map<String, Comment> mongoComments = this.comment_repo.findCommentsByIds(fetchedCommentsIds);

					//differenza insiemistica tra gli id dei post scaricati ora da reddit e quelli gia' presenti su Mongo
					fetchedCommentsIds.removeAll(mongoComments.keySet());

					List<Comment> commentsToPush = new ArrayList<Comment>(fetchedCommentsIds.size());

					for (String commentId : fetchedCommentsIds)
						commentsToPush.add(commentsMap.get(commentId));

					//DEBUG
					System.out.println("ID dei nuovi commenti: "+fetchedCommentsIds);
					System.out.println("ID dei commenti gia' presenti su Mongo: "+mongoComments.keySet());

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
				comment.setId("reddit_"+jsonCommentChildData.get("name").asText());
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
