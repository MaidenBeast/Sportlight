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

import radeon.data.MonthArrayWritable;
import radeon.data.MonthProductKeyWritable;
import radeon.data.MonthWritable;
import radeon.data.ProductArrayWritable;
import radeon.data.ProductWritable;
import radeon.data.ProductWritableList;
import radeon.utils.Writables;

public class RevenuesReducer_2 extends
		Reducer<Text, MonthWritable, Text, MonthArrayWritable> {

	public void reduce(Text key, Iterable<MonthWritable> values,
			Context context) throws IOException, InterruptedException {
		List<MonthWritable> ordered = Writables.makeMonthList(values);
		MonthWritable[] monthArray = ordered.toArray(new MonthWritable[0]);
		MonthArrayWritable arrayWritable = new MonthArrayWritable();
		arrayWritable.set(monthArray);
		context.write(key, arrayWritable);
	}
}
