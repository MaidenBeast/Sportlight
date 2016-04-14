package it.uniroma3.bigdata.radeon.mappers;

import it.uniroma3.bigdata.radeon.data.ProductWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BestFivePerMonthMapper extends
		Mapper<LongWritable, Text, Text, ProductWritable> {
	
	private final String delimiter = ",";
	private final String dateDelimiter = "-";
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, this.delimiter);
		
		String month = this.getMonth(st.nextToken());
		List<String> products = this.splitProducts(st);
		
		Text writeMonth = new Text(month);
		for (String prod : products) {
			context.write(writeMonth, new ProductWritable(prod, 1));
		}
	}
	
	private String getMonth(String date) {
		StringTokenizer st = new StringTokenizer(date, this.dateDelimiter);
		String year = st.nextToken();
		String month = st.nextToken();
		
		return year + "-" + month;
	}
	
	private List<String> splitProducts(StringTokenizer tokenizer) {
		List<String> products = new ArrayList<>();
		while (tokenizer.hasMoreTokens()) {
			products.add(tokenizer.nextToken());
		}
		return products;
	}
}