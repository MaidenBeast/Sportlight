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
import radeon.data.MonthProductKeyWritable;
import radeon.data.MonthWritable;
import radeon.utils.MonthComparator;

public class RevenuesReducer_1 extends
		Reducer<MonthProductKeyWritable, IntWritable, MonthProductKeyWritable, IntWritable> {

	public void reduce(MonthProductKeyWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		for (IntWritable cost : values) {
			sum += cost.get();
		}
		context.write(key, new IntWritable(sum));
	}
}