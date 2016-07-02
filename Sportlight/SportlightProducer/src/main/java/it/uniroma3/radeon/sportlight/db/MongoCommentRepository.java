package it.uniroma3.radeon.sportlight.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Filters.*;

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
			e.printStackTrace();
		}

	}

	@Override
	public Comment findCommentById(String id) {
		/*
		 * esempio query diretta su MongoDB
		 * db.getCollection("post").find(
		 * 		{
		 * 			"comments.id": "comment1" //query
		 * 		},
		 * 		{ //retrievedField
		 * 			_id: 0,
		 * 			comments:
		 * 			{$elemMatch: 
		 * 				{
		 * 					id: "comment1"
		 * 				}
		 * 			}
		 * 		}
		 * );
		 *
		 */
		
		Comment comment = null;
		
		ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");
		
		Bson query = eq("comments.id", id);
		Bson projection = elemMatch("comments.id");
		
		Document commentDoc = collection.find(query).projection(projection).first();
		String commentJson = commentDoc.toJson();
		
		try {
			JsonNode rootNode = mapper.readValue(commentJson, JsonNode.class);
			JsonNode commentsNode = rootNode.get("comments");
			Comment[] comments = mapper.readValue(commentsNode.traverse(), Comment[].class);
			comment = comments[0];
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return comment;
	}

}
