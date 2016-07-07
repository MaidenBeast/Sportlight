package it.uniroma3.radeon.storm.spouts;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;

public class KafkaSpoutBuilder {
	
	private BrokerHosts zkServer;
	private String topic;
	private String zkRoot;
	private String consumerGroup;
	
	public KafkaSpoutBuilder() {}
	
	public KafkaSpout buildSpout() {
		SpoutConfig sc = new SpoutConfig(this.zkServer, this.topic, this.zkRoot, this.consumerGroup);
		sc.scheme = new SchemeAsMultiScheme(new StringScheme());
		return new KafkaSpout(sc);
	}

	public BrokerHosts getZookeeperServer() {
		return zkServer;
	}

	public KafkaSpoutBuilder setZookeeperServer(String zkUrl) {
		this.zkServer = new ZkHosts(zkUrl);
		return this;
	}

	public String getTopic() {
		return topic;
	}

	public KafkaSpoutBuilder setTopic(String topic) {
		this.topic = topic;
		return this;
	}

	public String getZkRoot() {
		return zkRoot;
	}

	public KafkaSpoutBuilder setZkRoot(String zkRoot) {
		this.zkRoot = zkRoot;
		return this;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public KafkaSpoutBuilder setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
		return this;
	}
}
