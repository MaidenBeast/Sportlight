package it.uniroma3.radeon.sa.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parsing {
	
	public static Map<String, String> parseToMap(Map<String, String> model, String inputFile, String separator) {
		if (model.size() > 0) {
			model.clear();
		}
		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] keyValue = line.split(separator);
				model.put(keyValue[0], keyValue[1]);
			}
			return model;
		}
		catch (IOException e) {
			e.printStackTrace();
			return model;
		}
		catch (Exception e) {
			e.printStackTrace();
			return model;
		}
	}
	
	public static List<String> parseToList(List<String> model, String inputFile) {
		if (model.size() > 0) {
			model.clear();
		}
		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				model.add(line);
			}
			return model;
		}
		catch (IOException e) {
			e.printStackTrace();
			return model;
		}
		catch (Exception e) {
			e.printStackTrace();
			return model;
		}
	}
}
