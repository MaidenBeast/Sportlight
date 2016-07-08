package it.uniroma3.radeon.sa.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class PropertyLoader {
	
	public static Properties loadProperties(String fileName) {
		Properties prop = new Properties();
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			prop.load(br);
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return prop;
	}
}
