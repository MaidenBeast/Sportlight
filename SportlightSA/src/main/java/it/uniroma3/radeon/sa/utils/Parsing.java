package it.uniroma3.radeon.sa.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parsing {
	
	public static Map<String, String> ruleParser(String inputFile, String separator) {
		Map<String, String> raw2norm = new HashMap<>();
		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] keyValue = line.split(separator);
				String key = keyValue[0];
				String val = keyValue[1];
				raw2norm.put(key, val);
			}
			return raw2norm;
		}
		catch (IOException e) {
			e.printStackTrace();
			return raw2norm;
		}
		catch (Exception e) {
			e.printStackTrace();
			return raw2norm;
		}
	}
	
	public static Map<String, Integer> parseTopics(String text, String kvSeparator, String tupleSeparator) {
		Map<String, Integer> key2value = new HashMap<>();
		String[] tuples = text.split(tupleSeparator);
		for (String tuple : tuples) {
			String[] keyAndValue = tuple.split(kvSeparator);
			String key = keyAndValue[0];
			Integer value = Integer.parseInt(keyAndValue[1]);
			key2value.put(key, value);
		}
		return key2value;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> Map<String, T> parseTextToMap(String text, String kvSeparator, String tupleSeparator) {
		Map<String, T> key2value = new HashMap<>();
		String[] tuples = text.split(tupleSeparator);
		for (String tuple : tuples) {
			String[] keyAndValue = tuple.split(kvSeparator);
			String key = keyAndValue[0];
			T value = (T) keyAndValue[1];
			key2value.put(key, value);
		}
		return key2value;
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
