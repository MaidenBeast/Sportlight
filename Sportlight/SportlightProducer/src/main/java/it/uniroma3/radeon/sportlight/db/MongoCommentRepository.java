package it.uniroma3.radeon.sportlight.db;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import it.uniroma3.radeon.sportlight.data.Comment;
import it.uniroma3.radeon.sportlight.db.mongo.MongoDataSource;

public class MongoCommentRepository implements CommentRepository {
	private MongoDataSource mongoDataSource;

	public MongoCommentRepository() {
		this.mongoDataSource = new MongoDataSource();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");
		collection.createIndex(new Document("comment.id", 1)); //creo indice su comment.id
	}

	@Override
	public void persistOne(Comment comment) {
		ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");

		String post_id = comment.getPost().getId();
		try {
			Document commentDoc = Document.parse(mapper.writeValueAsString(comment));
			Document queryDoc = new Document("id", post_id);
			Document updateQueryDoc = new Document("$push", new Document("comments", commentDoc));
			collection.updateOne(queryDoc, updateQueryDoc);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void persistMany(List<Comment> comments) {
		ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");
		
		try {	
			List<WriteModel<Document>> bulkUpdateList = new ArrayList<WriteModel<Document>>(comments.size());
			
			for (Comment comment : comments) {
				String post_id = comment.getPost().getId();
				Document commentDoc = Document.parse(mapper.writeValueAsString(comment));
				Document queryDoc = new Document("id", post_id);
				Document updateQueryDoc = new Document("$push", new Document("comments", commentDoc));
				WriteModel<Document> updateModel = new UpdateOneModel<Document>(queryDoc, updateQueryDoc);
				bulkUpdateList.add(updateModel);
			}
			collection.bulkWrite(bulkUpdateList);
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
