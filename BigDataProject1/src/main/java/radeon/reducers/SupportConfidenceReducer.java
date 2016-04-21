package radeon.reducers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import radeon.data.PercentagesWritable;
import radeon.data.ProductPairWritable;

public class SupportConfidenceReducer extends
		Reducer<ProductPairWritable, IntWritable, ProductPairWritable, PercentagesWritable> {

	public void reduce(ProductPairWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		double coveredBills = 0.0;
		
		for (IntWritable num : values) {
			coveredBills += 1.0;
		}
		
		Counter billCounter = (Counter) context.getCounter("TOTALS", "BILLS_COUNTER");
		double support = coveredBills/billCounter.getValue();
		//double support = coveredBills/context.getCounter("TOTALS", "BILLS_COUNTER").getValue();
		
		String leftFood = key.getLeftFood().toString();
		Counter billProductCounter = (Counter)context.getCounter("TOTALS", "BILLS_WITH_" + leftFood.toUpperCase());
		double confidence = coveredBills/billProductCounter.getValue();
		//double confidence = coveredBills/context.getCounter("TOTALS", "BILLS_WITH_" + leftFood.toUpperCase()).getValue();
		
		PercentagesWritable result = new PercentagesWritable(new DoubleWritable(support), new DoubleWritable(confidence));
		context.write(key, result);	
	}
}