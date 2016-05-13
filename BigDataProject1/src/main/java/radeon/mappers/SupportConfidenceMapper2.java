package radeon.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import radeon.data.PercentagesWritable;
import radeon.data.ProductPairCountWritable;
import radeon.data.ProductPairWritable;
import radeon.utils.Parsing;

public class SupportConfidenceMapper2 extends
Mapper<LongWritable, Text, ProductPairWritable, PercentagesWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line);

		ProductPairCountWritable ppw = Parsing.getPairCount(st);
		
		int totalBills = conf.getInt("counters.totals.bills_counter", 0);
		//System.out.println("counters.totals.bills_counter = "+totalBills);
		double support = (double)ppw.getSupportCount().get()/totalBills;
		
		int billByProduct = conf.getInt("counters.totals.bills_with_"+ppw.getProductPair().getLeftFood(), 0);
		//System.out.println("counters.totals.bills_with_"+ppw.getProductPair().getLeftFood()+" = "+billByProduct);
		double confidence = (double)ppw.getSupportCount().get()/billByProduct;
		
		PercentagesWritable result = new PercentagesWritable(new DoubleWritable(support), new DoubleWritable(confidence));
		context.write(ppw.getProductPair(), result);
	}
}