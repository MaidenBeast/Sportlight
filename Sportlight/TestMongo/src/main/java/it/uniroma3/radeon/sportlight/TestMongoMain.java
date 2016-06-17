package it.uniroma3.radeon.sportlight;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class TestMongoMain {

	public static void main(String[] args) {
		/* 
		 * per aggiungere credenziali:
		 * http://stackoverflow.com/questions/4881208/how-to-put-username-password-in-mongodb
		 */
		//MongoCredential credential = MongoCredential.createCredential("sportlight", "sportlight", "sportlight".toCharArray());
		//List<MongoCredential> credentials = new ArrayList<MongoCredential>(Arrays.asList(credential));
		
		@SuppressWarnings("resource")
		//MongoClient mongoClient = new MongoClient(new ServerAddress("localhost"), credentials);
		MongoClient mongoClient = new MongoClient();
		MongoDatabase database = mongoClient.getDatabase("test_mongo");
		
		MongoCollection<Document> collection = database.getCollection("test");
		
		Document doc = new Document("name", "MongoDB")
	               .append("type", "database")
	               .append("count", 1)
	               .append("info", new Document("x", 203).append("y", 102));
		
		collection.insertOne(doc);
	}

}
