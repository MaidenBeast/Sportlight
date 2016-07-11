package it.uniroma3.radeon.sa.functions.mappers;

import java.util.ArrayList;
import java.util.List;

import it.uniroma3.radeon.sa.data.stateful.StateEntry;

public class FrequencyStateMapper extends StateDefinerMapper<Long> {
	
	private static final long serialVersionUID = 1L;
	
	private String countKey;
	private Long timestep;
	
	public FrequencyStateMapper(String countKey, Long timestep) {
		this.countKey = countKey;
		this.timestep = timestep;
	}

	public Iterable<StateEntry<Long>> call(Long count) throws Exception {
		List<StateEntry<Long>> stateEntries = new ArrayList<>();
		stateEntries.add(new StateEntry<Long>(this.countKey, count));
		stateEntries.add(new StateEntry<Long>("time", this.timestep));
		return stateEntries;
	}
}
