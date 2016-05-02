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
import radeon.utils.Writables;

public class BestFivePerMonthReducer_2_2 extends
		Reducer<Text, ProductWritable, Text, ProductArrayWritable> {

	public void reduce(Text key, Iterable<ProductWritable> values,
			Context context) throws IOException, InterruptedException {
		List<ProductWritable> best5 = Writables.takeBest(values, 5);
		ProductWritable[] best5Array = best5.toArray(new ProductWritable[0]);
		
		ProductArrayWritable writableArray = new ProductArrayWritable();
		writableArray.set(best5Array);
		context.write(key, writableArray);
	}
}
