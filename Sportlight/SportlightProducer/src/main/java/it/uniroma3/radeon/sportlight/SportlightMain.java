package it.uniroma3.radeon.sportlight;

import it.uniroma3.radeon.sportlight.modules.ModuleNotFoundException;
import it.uniroma3.radeon.sportlight.modules.ModuleThread;

public class SportlightMain {

	public static void main(String[] args) {
		@SuppressWarnings("unused")
		ModuleThread mThread;
		try {
			mThread = new ModuleThread("it.uniroma3.radeon.sportlight.modules.RedditModule");
		} catch (ModuleNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
