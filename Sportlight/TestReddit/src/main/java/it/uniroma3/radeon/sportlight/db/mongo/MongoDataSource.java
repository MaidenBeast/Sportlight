package it.uniroma3.radeon.sportlight.db.mongo;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDataSource {
	private MongoDatabase mongo_db;
	
	@SuppressWarnings("resource")
	public MongoDataSource() {
		/*
		 * I seguenti parametri poi dovranno essere letti esternamente da file:
		 * - i vari host di mongo;
		 * - database di riferimento;
		 * - autenticazione;
		 */
		this.mongo_db = new MongoClient().getDatabase("sportlight");
	}
	
	public MongoCollection<Document> getCollection(String collection) {
		return this.mongo_db.getCollection(collection);
	}
}
