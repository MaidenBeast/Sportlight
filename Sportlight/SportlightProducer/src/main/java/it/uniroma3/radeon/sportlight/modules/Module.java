package it.uniroma3.radeon.sportlight.modules;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.google.common.io.Resources;

import it.uniroma3.radeon.sportlight.db.CommentRepository;
import it.uniroma3.radeon.sportlight.db.MongoCommentRepository;
import it.uniroma3.radeon.sportlight.db.MongoPostRepository;
import it.uniroma3.radeon.sportlight.db.PostRepository;

public abstract class Module {
	protected KafkaProducer<String, String> producer;

	protected PostRepository post_repo;
	protected CommentRepository comment_repo;

	protected Module() {
		try (InputStream props = Resources.getResource("producer.props").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
			post_repo = new MongoPostRepository();
			comment_repo = new MongoCommentRepository();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		this.bootstrap();
		this.listen();
		this.producer.close();
	}
	
	protected abstract void bootstrap();
	protected abstract void listen();
}
