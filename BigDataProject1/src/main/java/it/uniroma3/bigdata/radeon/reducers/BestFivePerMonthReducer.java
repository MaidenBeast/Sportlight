package it.uniroma3.bigdata.radeon.reducers;

import it.uniroma3.bigdata.radeon.data.ProductWritable;
import it.uniroma3.bigdata.radeon.data.ProductWritableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BestFivePerMonthReducer extends
		Reducer<Text, ProductWritable, Text, ProductWritableList> {

	public void reduce(Text key, Iterable<ProductWritable> values,
			Context context) throws IOException, InterruptedException {
		Map<String, ProductWritable> map = new HashMap<>();
		for (ProductWritable pw : values) {
			String prodName = pw.getName();
			if (map.containsKey(prodName)) {
				ProductWritable trackedProd = map.get(prodName);
				int newCount = trackedProd.getCount() + 1;
				trackedProd.setCount(newCount);
			}
			else {
				map.put(prodName, pw);
			}
		}
		
		List<ProductWritable> products = new ArrayList<>(map.values());
		Collections.sort(products);
		List<ProductWritable> best5 = products.subList(0, 5);
		ProductWritableList writeBest5 = new ProductWritableList(best5);
		
		context.write(key, writeBest5);
	}
}