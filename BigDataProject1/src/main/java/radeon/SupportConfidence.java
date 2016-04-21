package radeon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import radeon.data.PercentagesWritable;
import radeon.data.ProductPairWritable;
import radeon.mappers.SupportConfidenceMapper;
import radeon.reducers.SupportConfidenceReducer;

public class SupportConfidence {

	public static void main(String[] args) throws Exception {
		
		//Probabilmente la specifica che il singolo record � una linea � contenuta nell'oggetto Configuration di default
		Job job = new Job(new Configuration(), "SupportConfidence");

		job.setJarByClass(SupportConfidence.class);
		
		job.setMapperClass(SupportConfidenceMapper.class);
		job.setReducerClass(SupportConfidenceReducer.class);
		
		//Setta il percorso dei dati di input e quello per i dati di output nell'hdfs
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(ProductPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(ProductPairWritable.class);
		job.setOutputValueClass(PercentagesWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.waitForCompletion(true);
	}
}