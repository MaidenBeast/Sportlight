package radeon.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import radeon.data.ProductArrayWritable;
import radeon.data.ProductWritable;
import radeon.data.ProductWritableList;

public class BestFivePerMonthReducer extends
		Reducer<Text, ProductWritable, Text, ProductArrayWritable> {

	public void reduce(Text key, Iterable<ProductWritable> values,
			Context context) throws IOException, InterruptedException {
		Map<String, ProductWritable> map = new HashMap<String, ProductWritable>();
		for (ProductWritable pw : values) {
			String prodName = pw.getName().toString();
			System.out.println(key.toString()+" "+pw.toString());
			
			ProductWritable trackedProd = null;
			
			if (map.containsKey(prodName)) {
				trackedProd = map.get(prodName);
				int newCount = trackedProd.getCount().get() + 1;
				trackedProd.setCount(new IntWritable(newCount));
			}
			else {
				trackedProd = new ProductWritable(new Text(prodName), new IntWritable(1));
				map.put(prodName, trackedProd);
			}
		}
		
		Writable[] products = map.values().toArray(new ProductWritable[0]);
	
		//debug su log
		/*System.out.print(key.toString()+" ");
		for (Writable w : products) {
			System.out.print(w.toString()+ " ");
		}
		System.out.println();*/
		
		Arrays.sort(products, Collections.reverseOrder()); //sort in ordine decrescente
		Writable[] best5 = Arrays.copyOf(products, 5);
		
		//debug su log
		System.out.print(key.toString()+" ");
		for (Writable w : best5) {
			System.out.print(w.toString()+ " ");
		}
		System.out.println();
		
		ProductArrayWritable writeBest5 = new ProductArrayWritable();
		writeBest5.set(best5);
		
		context.write(key, writeBest5);
	}
}