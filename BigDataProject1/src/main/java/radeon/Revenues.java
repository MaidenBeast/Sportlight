package radeon;

import radeon.data.MonthArrayWritable;
import radeon.data.MonthWritable;
import radeon.data.ProductArrayWritable;
import radeon.data.ProductWritable;
import radeon.data.ProductWritableList;
import radeon.mappers.BestFivePerMonthMapper;
import radeon.mappers.RevenuesMapper;
import radeon.reducers.BestFivePerMonthReducer;
import radeon.reducers.RevenuesReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Revenues {

	public static void main(String[] args) throws Exception {
		
		//Probabilmente la specifica che il singolo record � una linea � contenuta nell'oggetto Configuration di default
		Job job = new Job(new Configuration(), "Revenues");

		job.setJarByClass(Revenues.class);
		
		job.setMapperClass(RevenuesMapper.class);
		job.setReducerClass(RevenuesReducer.class);
		
		//Setta il percorso dei dati di input e quello per i dati di output nell'hdfs
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MonthWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MonthArrayWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.waitForCompletion(true);
	}
}