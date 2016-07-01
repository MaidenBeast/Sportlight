package it.uniroma3.radeon.sportlight.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
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
		collection.createIndex(new Document("comment.id", 1), new IndexOptions().unique(true)); //creo indice su comment.id
	}
	
	@Override
	public void persistOne(Post post) {
		ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");
		
		try {
			Document doc = Document.parse(mapper.writeValueAsString(post));
			collection.insertOne(doc);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
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
			collection.insertMany(docs);
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Post findByPostId(String id, boolean alsoComments) {
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
			post = mapper.readValue(postDoc.toJson(), Post.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return post;
	}

}
