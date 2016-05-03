package radeon;

import radeon.data.MonthProductKeyWritable;
import radeon.data.PercentagesWritable;
import radeon.data.ProductArrayWritable;
import radeon.data.ProductPairWritable;
import radeon.data.ProductWritable;
import radeon.data.ProductWritableList;
import radeon.mappers.BestFivePerMonthMapper;
import radeon.mappers.BestFivePerMonthMapper_2_1;
import radeon.mappers.BestFivePerMonthMapper_2_2;
import radeon.mappers.SupportConfidenceMapper;
import radeon.mappers.SupportConfidenceMapper2;
import radeon.reducers.BestFivePerMonthReducer;
import radeon.reducers.BestFivePerMonthReducer_2_1;
import radeon.reducers.BestFivePerMonthReducer_2_2;
import radeon.reducers.SupportConfidenceReducer;
import radeon.utils.Jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BestFivePerMonth_2 {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path temp = new Path("/temp");
		Path output = new Path(args[1]);

		Configuration conf = new Configuration();

		//Probabilmente la specifica che il singolo record � una linea � contenuta nell'oggetto Configuration di default
		Job job1 = new Job(conf, "BestFivePerMonth1");

		job1.setJarByClass(BestFivePerMonth_2.class);

		job1.setMapperClass(BestFivePerMonthMapper_2_1.class);
		job1.setReducerClass(BestFivePerMonthReducer_2_1.class);

		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, temp);

		job1.setMapOutputKeyClass(MonthProductKeyWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(MonthProductKeyWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		long startTime = System.currentTimeMillis();
		boolean succ = job1.waitForCompletion(true);

		if (!succ) {
			System.out.println("Job1 failed, exiting");
			System.exit(-1);
		}

		Job job2 = new Job(conf, "BestFivePerMonth2");
		job2.setJarByClass(BestFivePerMonth_2.class);
		job2.setMapperClass(BestFivePerMonthMapper_2_2.class);
		job2.setReducerClass(BestFivePerMonthReducer_2_2.class);

		FileInputFormat.addInputPath(job2, temp);
		FileOutputFormat.setOutputPath(job2, output);

		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(ProductWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(ProductArrayWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		succ = job2.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job2 failed, exiting");
			System.exit(-1);
		}
		
		System.out.println("Entire job finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds");
		
		//Pulisci la cartella temp per permettere esecuzioni future
		FileSystem fs = FileSystem.get(conf);
		fs.delete(temp);
	}
}