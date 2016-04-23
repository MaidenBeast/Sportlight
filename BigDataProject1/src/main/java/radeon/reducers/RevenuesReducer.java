package radeon.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import radeon.data.MonthArrayWritable;
import radeon.data.MonthWritable;
import radeon.utils.MonthComparator;

public class RevenuesReducer extends
		Reducer<Text, MonthWritable, Text, MonthArrayWritable> {

	public void reduce(Text key, Iterable<MonthWritable> values,
			Context context) throws IOException, InterruptedException {
		
		Map<String, MonthWritable> monthlyRevs = new TreeMap<String, MonthWritable>(new MonthComparator("-"));
		this.prepareRevs(monthlyRevs);
		for (MonthWritable mw : values) {
			String monthName = mw.getMonth().toString();
			
			MonthWritable trackedMonth = null;
			
			if (monthlyRevs.containsKey(monthName)) {
				trackedMonth = monthlyRevs.get(monthName);
				trackedMonth.addToSum(mw.getSumAsInt());
//				int newSum = trackedMonth.getSumAsInt() + mw.getSumAsInt();
//				trackedMonth.setSum(new IntWritable(newSum));
			}
			else {
				trackedMonth = new MonthWritable(new Text(monthName), mw.getSum());
				monthlyRevs.put(monthName, trackedMonth);
			}
		}
		
		Writable[] revenues = monthlyRevs.values().toArray(new MonthWritable[0]);
		MonthArrayWritable writeRevenues = new MonthArrayWritable();
		writeRevenues.set(revenues);
		
		context.write(key, writeRevenues);
		
	}
	
	private void prepareRevs(Map<String, MonthWritable> map) {
		for (int month = 1; month <= 12; month += 1) {
			String monthName = "2015-" + month;
			map.put(monthName, new MonthWritable(new Text(monthName), new IntWritable(0)));
		}
	}
	
}