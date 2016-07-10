package it.uniroma3.radeon.sportlight.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;

import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Filters.*;

import it.uniroma3.radeon.sportlight.data.Post;
import it.uniroma3.radeon.sportlight.db.mongo.MongoDataSource;

public class MongoPostRepository implements PostRepository {
	private MongoDataSource mongoDataSource;

	public MongoPostRepository() {
		this.mongoDataSource = new MongoDataSource();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");
		collection.createIndex(new Document("id", 1), new IndexOptions().unique(true)); //creo indice su id
		collection.createIndex(new Document("comment.id", 1)); //creo indice su comment.id
	}

	@Override
	public void persistOne(Post post) {
		ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");

		try {
			Document doc = Document.parse(mapper.writeValueAsString(post));
			collection.insertOne(doc);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void persistMany(List<Post> posts) {
		ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");

		try {	
			List<Document> docs = new ArrayList<Document>(posts.size());

			for (Post post : posts)
				docs.add(Document.parse(mapper.writeValueAsString(post)));
			if (docs.size() > 0)
				collection.insertMany(docs);

		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Post findPostById(String id, boolean alsoComments) {
		Post post = null;
		ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");

		List<String> excludeFields = new LinkedList<>();
		excludeFields.add("_id"); //intanto escludo il campo _id

		if (!alsoComments) //nel caso in cui io non voglia i commenti
			excludeFields.add("comments"); //escludi pure il campo "comments"

		Bson query = eq("id", id); //cerca per id
		Bson projection = exclude(excludeFields); //proiezione per esclusione 

		Document postDoc = collection.find(query).projection(projection).first();
		try {
			if (postDoc != null)
				post = mapper.readValue(postDoc.toJson(), Post.class);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return post;
	}

	@Override
	public Map<String, Post> findPostsByIds(Set<String> ids, boolean alsoComments) {
		/*
		 * Schema della query:
		 * db.getCollection("post").find(
		 * 		{"id": 
		 * 			{ $in : [
		 * 				"post1",
		 * 				"post2"
		 * 				] 
		 * 			}
		 * 		}, 
		 * 		{ 
		 * 			"comments": 0
		 * 		}
		 * );
		 */

		final Map<String, Post> postMap = new HashMap<String, Post>(ids.size());

		final ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");

		List<String> excludeFields = new LinkedList<>();
		excludeFields.add("_id"); //intanto escludo il campo _id

		if (!alsoComments) //nel caso in cui io non voglia i commenti
			excludeFields.add("comments"); //escludi pure il campo "comments"

		Bson query = new Document("id", new Document("$in", ids));
		Bson projection = exclude(excludeFields); //proiezione per esclusione 

		FindIterable<Document> iterable = collection.find(query).projection(projection);

		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				try {
					Post post = mapper.readValue(document.toJson(), Post.class);
					postMap.put(post.getId(), post);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		return postMap;
	}

	@Override
	public Map<String, Post> findAllPosts(boolean alsoComments) {
		return this.findAllPostsBySrcs(null, alsoComments);
	}

	@Override
	public Map<String, Post> findAllPostsBySrcs(List<String> srcs, boolean alsoComments) {
		final Map<String, Post> postMap = new HashMap<String, Post>();

		final ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");

		List<String> excludeFields = new LinkedList<>();
		excludeFields.add("_id"); //intanto escludo il campo _id

		if (!alsoComments) //nel caso in cui io non voglia i commenti
			excludeFields.add("comments"); //escludi pure il campo "comments"

		Bson query = null;
		Bson projection = exclude(excludeFields); //proiezione per esclusione
		MongoCursor<Document> cursor;
		
		if (srcs != null && srcs.size()>0) { //nel caso in cui la lista srcs sia stata riempita
			query = new Document("src", new Document("$in", srcs)); //cerca effettivamente per srcs
			cursor = collection.find(query)
					.projection(projection)
					.iterator();
		} else { //si vogliono cercare TUTTI i post (caso particolare)
			cursor = collection.find()
					.projection(projection)
					.iterator();
		}

		try {
			while (cursor.hasNext()) {
				Document document = cursor.next();
				Post post = mapper.readValue(document.toJson(), Post.class);
				postMap.put(post.getId(), post);
			}
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			cursor.close();
		}

		return postMap;
	}

}
