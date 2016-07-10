package it.uniroma3.radeon.sportlight.db;

import it.uniroma3.radeon.sportlight.data.State;

public interface StateRepository {
	public void updateState(State state);
	public State getStateBySrc(String src);
}
