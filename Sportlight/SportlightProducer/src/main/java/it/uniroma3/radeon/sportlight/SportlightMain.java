package it.uniroma3.radeon.sportlight;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import it.uniroma3.radeon.sportlight.modules.ModuleNotFoundException;
import it.uniroma3.radeon.sportlight.modules.ModuleThread;

public class SportlightMain {

	public static void main(String[] args) {
		Properties prop = new Properties();
		InputStream input = null;
		
		try {
			input = new FileInputStream("modules.properties");
			prop.load(input);
			
			/*
			 * per ogni modulo definito nel file properties
			 * crea un suo thread separato
			 */
			for (String moduleName : prop.stringPropertyNames())
				new ModuleThread(prop.getProperty(moduleName));
			
		} catch (ModuleNotFoundException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
