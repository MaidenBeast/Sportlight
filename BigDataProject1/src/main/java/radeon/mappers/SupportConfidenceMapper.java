package radeon.mappers;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import radeon.data.MonthWritable;
import radeon.data.ProductPairWritable;
import radeon.data.ProductWritable;
import radeon.utils.Parsing;
import radeon.utils.Writables;
import radeon.utils.counters.Totals;


public class SupportConfidenceMapper extends
		Mapper<LongWritable, Text, ProductPairWritable, IntWritable> {
	
	private final String delimiter = ",";
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, this.delimiter);
		
		//Muoviti oltre il mese
		Parsing.skipFields(st, 1);
		
		Set<String> products = Parsing.splitDistinctProducts(st);
		Set<ProductPairWritable> prodPairs = Writables.generatePairs(products);
		
		context.getCounter("TOTALS", "BILLS_COUNTER").increment(1);
		for (String p : products) {
			context.getCounter("TOTALS", "BILLS_WITH_" + p.toUpperCase()).increment(1);
		}
		
		for (ProductPairWritable pair : prodPairs) {
			context.write(pair, new IntWritable(1));
		}
	}
}