package radeon.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import radeon.data.MonthProductKeyWritable;
import radeon.data.ProductWritable;
import radeon.utils.Parsing;


public class BestFivePerMonthMapper_2_2 extends
		Mapper<MonthProductKeyWritable, IntWritable, Text, ProductWritable> {
	
	public void map(MonthProductKeyWritable key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		
		Text month = key.getMonth();
		ProductWritable productInfo = new ProductWritable(key.getProduct(), value);
		context.write(month, productInfo);
	}
}