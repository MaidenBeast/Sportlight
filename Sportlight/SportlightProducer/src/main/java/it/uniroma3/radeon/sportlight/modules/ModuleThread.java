package it.uniroma3.radeon.sportlight.modules;

public class ModuleThread extends Thread {
	private Module module;
	
	public ModuleThread(String className) throws ModuleNotFoundException {
		try {
			this.module = (Module) Class.forName(className).newInstance();
			this.start();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			throw new ModuleNotFoundException("Modulo "+className+" non trovato");
		}
	}
	
	public void run() {
		this.module.run();
	}
}
