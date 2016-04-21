package radeon.mappers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import radeon.data.ProductPairWritable;
import radeon.utils.Parsing;
import radeon.utils.Writables;


public class SupportConfidenceMapper extends
		Mapper<LongWritable, Text, ProductPairWritable, IntWritable> {
	
	private final String delimiter = ",";
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, this.delimiter);
		
		//Muoviti oltre il mese
		Parsing.skipFields(st, 1);
		
		Set<String> products = Parsing.splitDistinctProducts(st);
		Set<ProductPairWritable> prodPairs = Writables.generatePairs(products);
		
		Counter billCounter = (Counter) context.getCounter("TOTALS", "BILLS_COUNTER");
		billCounter.increment(1L);
		
		for (String p : products) {
			//System.out.print(p + " ");	
			Counter billProductCounter = (Counter) context.getCounter("TOTALS", "BILLS_WITH_" + p.toUpperCase());
			billProductCounter.increment(1L);
		}
		
		//System.out.println();
		
		for (ProductPairWritable pair : prodPairs) {
			System.out.println(pair);
			context.write(pair, new IntWritable(1));
		}
	}
}