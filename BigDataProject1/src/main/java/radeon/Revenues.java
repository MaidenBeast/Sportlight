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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

public class Revenues {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Properties costs = Revenues.prepareCosts("/user/"
				+UserGroupInformation.getCurrentUser().getUserName()+
				"/input/costs.properties", conf);
		
		//Properties costs = Revenues.prepareCosts("/input/costs.properties", conf);
		
		for (Entry<Object, Object> entry : costs.entrySet()) {
			String propName = (String)entry.getKey();
		    String propValue = (String)entry.getValue();
		    conf.set(propName, propValue);
		    //System.out.println(propName +" = "+propValue);
		}
		
		//Probabilmente la specifica che il singolo record � una linea � contenuta nell'oggetto Configuration di default
		Job job = new Job(conf, "Revenues");
		
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
		System.out.println("Tempo impiegato: " + Jobs.getCompletionTime(job));
	}
	
	private static Properties prepareCosts(String filename, Configuration conf) throws IOException {
		Properties costs = new Properties();
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(filename);
		costs.load(fs.open(p));
		return costs;
	}
}
