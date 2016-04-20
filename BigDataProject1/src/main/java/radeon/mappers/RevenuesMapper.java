package radeon.mappers;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import radeon.data.MonthWritable;
import radeon.data.ProductWritable;
import radeon.utils.Parsing;


public class RevenuesMapper extends
		Mapper<LongWritable, Text, Text, MonthWritable> {
	
	private final String delimiter = ",";
	private final String dateDelimiter = "-";
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Properties costs = this.prepareCosts("d:/Hadoop/hadoop-2.7.1/jars/costs.properties");
//		Properties costs = new Properties();
//		costs.load(new FileInputStream("costs.properties"));

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, this.delimiter);
		
		String month = "2015-" + Parsing.getMonth(st.nextToken(), this.dateDelimiter);
		List<String> products = Parsing.splitProducts(st);
		
		for (String prod : products) {
			int cost = Integer.parseInt(costs.getProperty(prod));
			context.write(new Text(prod), new MonthWritable(new Text(month), new IntWritable(cost)));
		}
	}
	
	private Properties prepareCosts(String filename) throws IOException {
		Properties costs = new Properties();
		costs.load(new FileInputStream(filename));
		return costs;
	}
}