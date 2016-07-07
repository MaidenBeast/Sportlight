package it.uniroma3.radeon.sa.data.shared.accumulators;

import java.util.Map;

import org.apache.spark.AccumulatorParam;

public class SentimentCountAccumulator implements AccumulatorParam<Map<String, Integer>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Map<String, Integer> addInPlace(Map<String, Integer> counts1,
			Map<String, Integer> counts2) {
		for (String sentiment : counts2.keySet()) {
			Integer newCount = counts2.get(sentiment);
			if (counts1.containsKey(sentiment)) {
				Integer oldCount = counts1.get(sentiment);
				counts1.put(sentiment, oldCount + newCount);
			}
			else {
				counts1.put(sentiment, newCount);
			}
		}
		return counts1;
	}

	@Override
	public Map<String, Integer> zero(Map<String, Integer> emptyMap) {
		return emptyMap;
	}

	@Override
	public Map<String, Integer> addAccumulator(Map<String, Integer> counts1,
			Map<String, Integer> counts2) {
		return addInPlace(counts1, counts2);
	}

}
