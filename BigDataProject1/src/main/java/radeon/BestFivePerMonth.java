package radeon;

import radeon.data.ProductArrayWritable;
import radeon.data.ProductWritable;
import radeon.data.ProductWritableList;
import radeon.mappers.BestFivePerMonthMapper;
import radeon.reducers.BestFivePerMonthReducer;
import radeon.utils.Jobs;

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

public class BestFivePerMonth {

	public static void main(String[] args) throws Exception {
		
		//Probabilmente la specifica che il singolo record � una linea � contenuta nell'oggetto Configuration di default
		Job job = new Job(new Configuration(), "BestFivePerMonth");

		job.setJarByClass(BestFivePerMonth.class);
		
		job.setMapperClass(BestFivePerMonthMapper.class);
		job.setReducerClass(BestFivePerMonthReducer.class);
		
		//Setta il percorso dei dati di input e quello per i dati di output nell'hdfs
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ProductWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ProductArrayWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		
		System.out.println("Entire job finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds");
		
		//System.out.println("Tempo impiegato: " + Jobs.getCompletionTime(job));
	}
}