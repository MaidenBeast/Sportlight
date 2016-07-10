package it.uniroma3.radeon.sa.main.storm;

import it.uniroma3.radeon.sa.utils.PropertyLoader;
import it.uniroma3.radeon.storm.bolts.TestWordMapperBolt;
import it.uniroma3.radeon.storm.bolts.TestWordReducerBolt;
import it.uniroma3.radeon.storm.spouts.KafkaSpoutBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class TestTopology {
	
	public static void main(String[] args) {
		String configFile = args[0];
		Properties prop = PropertyLoader.loadProperties(configFile);
		
		//Settaggio del KafkaSpout
		KafkaSpoutBuilder spoutBuilder = new KafkaSpoutBuilder()
		                                     .setZookeeperServer(prop.getProperty("zkServer"))
		                                     .setTopic(prop.getProperty("topic"))
		                                     .setZkRoot(prop.getProperty("zkRoot"))
		                                     .setConsumerGroup(prop.getProperty("consumerGroup"));
		
		KafkaSpout spout = spoutBuilder.buildSpout();
		
		//Definizione della topologia
		TopologyBuilder topology = new TopologyBuilder();
		
		topology.setSpout("kafkaSpout", spout);
		topology.setBolt("wordMapper", new TestWordMapperBolt())
		        .shuffleGrouping("kafkaSpout");
		topology.setBolt("wordReducer", new TestWordReducerBolt())
		        .fieldsGrouping("wordMapper", new Fields("word"));
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, topology.createTopology());
		Utils.sleep(20 * 1000);
		cluster.killTopology("test");
		cluster.shutdown();
	}
}
