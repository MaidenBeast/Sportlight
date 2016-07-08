package it.uniroma3.radeon.storm.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("rawtypes")
public class TestWordMapperBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		String[] words = tuple.getString(0).split(" ");
		for (String w : words) {
			this.collector.emit(tuple, new Values(w, 1));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
}
