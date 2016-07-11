package it.uniroma3.radeon.sa.data.stateful;

import java.io.Serializable;

public class StateEntry<V> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String stateKey;
	private V stateValue;
	
	public StateEntry(String stateKey, V stateValue) {
		this.stateKey = stateKey;
		this.stateValue = stateValue;
	}

	public V getStateValue() {
		return stateValue;
	}

	public void setStateValue(V stateValue) {
		this.stateValue = stateValue;
	}

	public String getStateKey() {
		return stateKey;
	}
}
