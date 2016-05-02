package radeon.mappers;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import radeon.data.MonthProductKeyWritable;
import radeon.data.MonthWritable;
import radeon.utils.Parsing;


public class RevenuesMapper_1 extends
		Mapper<LongWritable, Text, MonthProductKeyWritable, IntWritable> {
	
	private final String delimiter = ",";
	private final String dateDelimiter = "-";
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, this.delimiter);
		
		String month = /*"2015-" +*/ Parsing.getMonth(st.nextToken(), this.dateDelimiter);
		List<String> products = Parsing.splitProducts(st);
		
		for (String prod : products) {
			MonthProductKeyWritable monthProd = new MonthProductKeyWritable(new Text(month), new Text(prod));
			int cost = Integer.parseInt(conf.get(prod));
			context.write(monthProd, new IntWritable(cost));
		}
	}
	
}