package it.uniroma3.radeon.sportlight.db.mongo;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.exclude;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;

import it.uniroma3.radeon.sportlight.data.State;
import it.uniroma3.radeon.sportlight.db.StateRepository;

public class MongoStateRepository implements StateRepository {
	private MongoDataSource mongoDataSource;
	
	public MongoStateRepository() {
		this.mongoDataSource = new MongoDataSource();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("state");
		collection.createIndex(new Document("src", 1), new IndexOptions().unique(true)); //creo indice su src
	}
	
	@Override
	public void updateState(State state) {
		ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("state");
		
		String src = state.getSrc();
		
		try {
			Document stateDoc = Document.parse(mapper.writeValueAsString(state));
			Document setDoc = new Document("$set", stateDoc);
			Document filterDoc = new Document("src", src);
			UpdateOptions updateOpts = new UpdateOptions();
			updateOpts.upsert(true);
			
			collection.updateOne(filterDoc, setDoc, updateOpts);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public State getStateBySrc(String src) {
		State state = null;
		ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("state");

		List<String> excludeFields = new LinkedList<>();
		excludeFields.add("_id"); //escludo il campo _id
		
		Bson query = eq("src", src);
		Bson projection = exclude(excludeFields); //proiezione per esclusione 
		
		Document stateDoc = collection.find(query).projection(projection).first();
		
		if (stateDoc != null) {
			String stateJson = stateDoc.toJson();
			try {
				state = mapper.readValue(stateJson, State.class);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return state;
	}

}
