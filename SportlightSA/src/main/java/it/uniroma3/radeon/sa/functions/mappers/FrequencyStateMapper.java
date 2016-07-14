package it.uniroma3.radeon.sa.functions.mappers;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

public class FrequencyStateMapper extends StateDefinerMapper<Long> {
	
	private static final long serialVersionUID = 1L;
	
	private String countKey;
	private Long timestep;
	
	public FrequencyStateMapper(String countKey, Long timestep) {
		this.countKey = countKey;
		this.timestep = timestep;
	}

	public Iterable<Tuple2<String, Long>> call(Long count) throws Exception {
		List<Tuple2<String, Long>> stateEntries = new ArrayList<>();
		stateEntries.add(new Tuple2<>(this.countKey, count));
		stateEntries.add(new Tuple2<>("time", this.timestep));
		return stateEntries;
	}
}
