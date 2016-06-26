package it.uniroma3.radeon.sportlight;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class Producer {
    public static void main(String[] args) throws IOException {
        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        try {
            for (int i = 0; i < 1000000; i++) {
            	ObjectMapper mapper = new ObjectMapper();
            	Message fastMessage = new Message();
            	
            	fastMessage.setType("fast");
            	fastMessage.setT(String.format(Locale.ENGLISH, "%.3f", System.nanoTime() * 1e-9));
            	fastMessage.setK(String.format("%d", i));
            	
                // send lots of messages
                producer.send(new ProducerRecord<String, String>("fast-messages",mapper.writeValueAsString(fastMessage)));

                // every so often send to a different topic
                if (i % 1000 == 0) {
                	Message slowMessage = new Message();
                	
                	slowMessage.setType("slow");
                	slowMessage.setT(String.format(Locale.ENGLISH, "%.3f", System.nanoTime() * 1e-9));
                	slowMessage.setK(String.format("%d", i));
                	
                    producer.send(new ProducerRecord<String, String>("slow-messages", mapper.writeValueAsString(slowMessage)));
                    producer.flush();
                    System.out.println("Sent msg number " + i);
                }
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

    }
}

