package it.uniroma3.radeon.sportlight.db;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.elemMatch;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.Block;
import com.mongodb.client.AggregateIterable;
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

	@Override
	public List<Comment> findCommentsByIds(List<String> ids) {
		/* 
		 * TODO Implementare questo tipo di query
		 * db.getCollection("post").aggregate([
		 *		{$match: {"comments.id": { $in : ["comment1", "comment2", "comment3", "comment5"] }}},
		 *		{
		 *			$project: {
		 * 				comments: {
		 *   				$filter: {
		 *     					input: "$comments",
		 *    					as: "comment",
		 *    					cond: { $or: [
		 *    								{ $eq: ["$$comment.id","comment1"] },
		 *    								{ $eq: ["$$comment.id","comment2"] },
		 *    								{ $eq: ["$$comment.id","comment3"] },
		 *    								{ $eq: ["$$comment.id","comment5"] }
		 *    							] 
		 *    						}
		 *   				}
		 *				}
		 *			}
		 *		}
		 *	]);
		 */
		
		List<Comment> comments = new ArrayList<Comment>(ids.size());
		
		ObjectMapper mapper = new ObjectMapper();
		MongoCollection<Document> collection = this.mongoDataSource.getCollection("post");
		
		AggregateIterable<Document> iterable = collection.aggregate(asList(
		        new Document("$match",
		        				new Document("comments.id",
		        						new Document("$in",
		        								asList("comment1", "comment2", "comment3", "comment5")
		        						)
		        				)
		        			),
		        new Document("$project",
		        				new Document("comments",
		        						new Document("$filter",
		        								new Document("input", "$comments")
		        								.append("as", "comment")
		        								.append("cond", new Document(
			        										"$or", asList(
			        												new Document("$eq", asList("$$comment.id","comment1")),
			        												new Document("$eq", asList("$$comment.id","comment2")),
			        												new Document("$eq", asList("$$comment.id","comment3")),
			        												new Document("$eq", asList("$$comment.id","comment5"))
			        											)
		        											)
		        										)
		        								)
		        						)
		        				)
		        ));
		        										
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document.toJson());
		    }
		});

		
		return comments;
	}

}
