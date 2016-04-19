package radeon.reducers;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import radeon.data.MonthArrayWritable;
import radeon.data.MonthWritable;
import radeon.data.PercentagesWritable;
import radeon.data.ProductArrayWritable;
import radeon.data.ProductPairWritable;
import radeon.data.ProductWritable;
import radeon.data.ProductWritableList;

public class SupportConfidenceReducer extends
		Reducer<ProductPairWritable, IntWritable, ProductPairWritable, PercentagesWritable> {

	public void reduce(ProductPairWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
	}
}