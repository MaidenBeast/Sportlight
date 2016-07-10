package it.uniroma3.radeon.sportlight.db.mongo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
		Properties prop = new Properties();
		InputStream input = null;
		
		try {
			input = new FileInputStream("mongo.properties");
			prop.load(input);
			
			String mongo_host = prop.getProperty("mongo.host");
			String mongo_port = prop.getProperty("mongo.port");
			String mongo_db = prop.getProperty("mongo.database");
			
			this.mongo_db = new MongoClient(mongo_host, Integer.parseInt(mongo_port))
							.getDatabase(mongo_db);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public MongoCollection<Document> getCollection(String collection) {
		return this.mongo_db.getCollection(collection);
	}
}
