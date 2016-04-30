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

import radeon.data.MonthProductKeyWritable;
import radeon.data.ProductArrayWritable;
import radeon.data.ProductWritable;
import radeon.data.ProductWritableList;

public class BestFivePerMonthReducer_2_1 extends
		Reducer<MonthProductKeyWritable, IntWritable, MonthProductKeyWritable, IntWritable> {

	public void reduce(MonthProductKeyWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable one : values) {
			sum += 1;
		}
		context.write(key, new IntWritable(sum));
	}
}
