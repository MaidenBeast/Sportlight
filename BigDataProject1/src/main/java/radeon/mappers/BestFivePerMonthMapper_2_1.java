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


public class BestFivePerMonthMapper_2_1 extends
		Mapper<LongWritable, Text, MonthProductKeyWritable, IntWritable> {
	
	private final String delimiter = ",";
	private final String dateDelimiter = "-";
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, this.delimiter);
		
		String month = Parsing.getMonth(st.nextToken(), this.dateDelimiter);
		List<String> products = Parsing.splitProducts(st);
		
		Text writeMonth = new Text(month);
		for (String prod : products) {
			Text writeProd = new Text(prod);
			context.write(new MonthProductKeyWritable(writeMonth, writeProd), new IntWritable(1));
		}
	}
}