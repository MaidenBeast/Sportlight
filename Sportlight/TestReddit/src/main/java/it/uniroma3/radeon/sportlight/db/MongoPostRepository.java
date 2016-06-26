package it.uniroma3.radeon.sportlight.db;

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
		// TODO Auto-generated method stub
		
	}

}
