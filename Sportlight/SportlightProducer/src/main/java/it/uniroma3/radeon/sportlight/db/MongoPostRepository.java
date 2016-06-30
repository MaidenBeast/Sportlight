package it.uniroma3.radeon.sportlight.db;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;

import it.uniroma3.radeon.sportlight.data.Post;
import it.uniroma3.radeon.sportlight.db.mongo.MongoDataSource;

public class MongoPostRepository implements PostRepository {
	private MongoDataSource mongoDataSource;
	
	public MongoPostRepository() {
		this.mongoDataSource = new MongoDataSource();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");
		collection.createIndex(new Document("id", 1)); //creo indice su id
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

}
