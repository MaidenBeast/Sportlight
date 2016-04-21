package radeon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import radeon.data.PercentagesWritable;
import radeon.data.ProductPairWritable;
import radeon.mappers.SupportConfidenceMapper;
import radeon.mappers.SupportConfidenceMapper2;
import radeon.reducers.SupportConfidenceReducer;

public class SupportConfidence {

	public static void main(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path temp = new Path("temp");
		Path output = new Path(args[1]);

		Configuration conf = new Configuration();

		//Probabilmente la specifica che il singolo record � una linea � contenuta nell'oggetto Configuration di default
		Job job1 = new Job(conf, "SupportConfidence1");

		job1.setJarByClass(SupportConfidence.class);

		job1.setMapperClass(SupportConfidenceMapper.class);
		job1.setReducerClass(SupportConfidenceReducer.class);

		//Setta il percorso dei dati di input e quello per i dati di output nell'hdfs
		FileInputFormat.addInputPath(job1, input);
		//FileOutputFormat.setOutputPath(job1, output);
		FileOutputFormat.setOutputPath(job1, temp);

		job1.setMapOutputKeyClass(ProductPairWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(ProductPairWritable.class);
		//job1.setOutputValueClass(PercentagesWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		boolean succ = job1.waitForCompletion(true);

		if (!succ) {
			System.out.println("Job1 failed, exiting");
			System.exit(-1);
		}

		Counters counters = job1.getCounters();
		CounterGroup cGroup = counters.getGroup("TOTALS");
		
		for (Counter c : cGroup) {
			conf.setInt("counters.totals."+c.getName().toLowerCase(), (int) c.getValue());
			/*System.out.println("counters.totals."+c.getName().toLowerCase()
					+ " = "+conf.getInt("counters.totals."+c.getName().toLowerCase(), 0));*/
		}
		
		/*for (CounterGroup group : counters) {
			group = (CounterGroup)group;
			if (group.getName().equals("TOTAL")) {
				for (Counter c : group) {
					conf.setInt("counters.total."+c.getName().toLowerCase(), (int) c.getValue());
				}
			}
		}*/

		Job job2 = new Job(conf, "SupportConfidence2");

		job2.setJarByClass(SupportConfidence.class);
		job2.setMapperClass(SupportConfidenceMapper2.class);
		//job2.setReducerClass(IdentityReducer.class);

		FileInputFormat.addInputPath(job2, temp);
		FileOutputFormat.setOutputPath(job2, output);

		job2.setMapOutputKeyClass(ProductPairWritable.class);
		job2.setMapOutputValueClass(PercentagesWritable.class);
		job2.setOutputKeyClass(ProductPairWritable.class);
		job2.setOutputValueClass(PercentagesWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		succ = job2.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job2 failed, exiting");
			System.exit(-1);
		}
	}
}