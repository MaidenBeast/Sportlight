package radeon.reducers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import radeon.data.PercentagesWritable;
import radeon.data.ProductPairWritable;

public class SupportConfidenceReducer extends
		Reducer<ProductPairWritable, IntWritable, ProductPairWritable, IntWritable> {

	public void reduce(ProductPairWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		int coveredBills = 0;
		
		for (IntWritable num : values) {
			coveredBills++;
		}
		
		//double support = coveredBills/billCounter.getValue();
		//double support = coveredBills/context.getCounter("TOTALS", "BILLS_COUNTER").getValue();
		
		//String leftFood = key.getLeftFood().toString();
		
		//double confidence = coveredBills/billProductCounter.getValue();
		//double confidence = coveredBills/context.getCounter("TOTALS", "BILLS_WITH_" + leftFood.toUpperCase()).getValue();
		
		//PercentagesWritable result = new PercentagesWritable(new DoubleWritable(support), new DoubleWritable(confidence));
		//context.write(key, result);
		
		context.write(key, new IntWritable(coveredBills));
	}
}