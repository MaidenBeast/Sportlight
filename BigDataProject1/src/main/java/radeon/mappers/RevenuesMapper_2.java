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


public class RevenuesMapper_2 extends
		Mapper<MonthProductKeyWritable, IntWritable, Text, MonthWritable> {
	
	public void map(MonthProductKeyWritable key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		
		Text product = key.getProduct();
		MonthWritable monthInfo = new MonthWritable(key.getMonth(), value);
		context.write(product, monthInfo);
	}
	
}