package radeon;

import radeon.data.MonthArrayWritable;
import radeon.data.MonthProductKeyWritable;
import radeon.data.MonthWritable;
import radeon.data.ProductArrayWritable;
import radeon.data.ProductWritable;
import radeon.data.ProductWritableList;
import radeon.mappers.BestFivePerMonthMapper;
import radeon.mappers.RevenuesMapper;
import radeon.mappers.RevenuesMapper_1;
import radeon.mappers.RevenuesMapper_2;
import radeon.reducers.BestFivePerMonthReducer;
import radeon.reducers.RevenuesReducer;
import radeon.reducers.RevenuesReducer_1;
import radeon.reducers.RevenuesReducer_2;
import radeon.utils.Jobs;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

public class Revenues_2 {

	public static void main(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path temp = new Path("/temp");
		Path output = new Path(args[1]);
		
		Configuration conf = new Configuration();
		Properties costs = Revenues_2.prepareCosts("/user/"
				+UserGroupInformation.getCurrentUser().getUserName()+
				"/input/costs.properties", conf);
		
		for (Entry<Object, Object> entry : costs.entrySet()) {
			String propName = (String)entry.getKey();
		    String propValue = (String)entry.getValue();
		    conf.set(propName, propValue);
		    //System.out.println(propName +" = "+propValue);
		}
		
		//Probabilmente la specifica che il singolo record � una linea � contenuta nell'oggetto Configuration di default
		Job job1 = new Job(conf, "Revenues1");
		
		job1.setJarByClass(Revenues_2.class);
		
		job1.setMapperClass(RevenuesMapper_1.class);
		job1.setReducerClass(RevenuesReducer_1.class);
		
		//Setta il percorso dei dati di input e quello per i dati di output nell'hdfs
		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, temp);

		job1.setMapOutputKeyClass(MonthProductKeyWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(MonthProductKeyWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		long startTime = System.currentTimeMillis();
		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf, "Revenues2");
		
		job2.setJarByClass(Revenues_2.class);
		
		job2.setMapperClass(RevenuesMapper_2.class);
		job2.setReducerClass(RevenuesReducer_2.class);
		
		FileInputFormat.addInputPath(job2, temp);
		FileOutputFormat.setOutputPath(job2, output);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(MonthWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(MonthArrayWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		System.out.println("Entire job finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds");
		
		//Pulisci la cartella temp per permettere esecuzioni future
		FileSystem fs = FileSystem.get(conf);
		fs.delete(temp);
	}
	
	private static Properties prepareCosts(String filename, Configuration conf) throws IOException {
		Properties costs = new Properties();
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(filename);
		costs.load(fs.open(p));
		return costs;
	}
}
