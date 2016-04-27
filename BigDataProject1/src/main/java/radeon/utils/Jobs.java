package radeon.utils;

import org.apache.hadoop.mapreduce.Job;

public class Jobs {
	
	public static long getCompletionTime(Job job) throws Exception {
		return job.getFinishTime() - job.getStartTime();
	}

}
