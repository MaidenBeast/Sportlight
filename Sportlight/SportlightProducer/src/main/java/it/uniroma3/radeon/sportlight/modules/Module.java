package it.uniroma3.radeon.sportlight.modules;

import it.uniroma3.radeon.sportlight.db.CommentRepository;
import it.uniroma3.radeon.sportlight.db.PostRepository;
import it.uniroma3.radeon.sportlight.db.StateRepository;
import it.uniroma3.radeon.sportlight.db.mongo.MongoCommentRepository;
import it.uniroma3.radeon.sportlight.db.mongo.MongoPostRepository;
import it.uniroma3.radeon.sportlight.db.mongo.MongoStateRepository;

public abstract class Module {

	protected PostRepository post_repo;
	protected CommentRepository comment_repo;
	protected StateRepository state_repo;

	protected Module() {
		post_repo = new MongoPostRepository();
		comment_repo = new MongoCommentRepository();
		state_repo = new MongoStateRepository();
	}
	
	public void run() {
		this.bootstrap();
		this.listen();
	}
	
	protected abstract void bootstrap();
	protected abstract void listen();
}
