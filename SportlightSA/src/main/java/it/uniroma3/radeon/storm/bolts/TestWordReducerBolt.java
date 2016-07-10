package it.uniroma3.radeon.storm.bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("rawtypes")
public class TestWordReducerBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private static Map<String, Integer> wordCounter = new HashMap<>();
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		Integer count = tuple.getInteger(1);
		
		if (wordCounter.containsKey(word)) {
			Integer oldTotal = wordCounter.get(word);
			wordCounter.put(word, oldTotal + count);
		}
		else {
			wordCounter.put(word, count);
		}
		this.collector.emit(tuple, new Values(word, wordCounter.get(word)));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "total"));
	}
}
