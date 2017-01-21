package UIS.BigDataAnalytics.Assignment1;

import org.apache.hadoop.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;


public class MostFrequentDestDriver extends Configured implements Tool  {
	public  static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitcode=ToolRunner.run(new Configuration(), new MostFrequentDestDriver(),args);
		System.exit(exitcode);
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job jobTracker=new Job(getConf());
		jobTracker.setJarByClass(MostFrequentDestDriver.class);
		jobTracker.setJobName("MostFrequentDest");
		jobTracker.setMapOutputKeyClass(Text.class);
		jobTracker.setMapOutputValueClass(Text.class);
		jobTracker.setMapperClass(MostFrequentDestMapper.class);
		jobTracker.setReducerClass(MostFrequentDestReducer.class);
		//jobTracker.setCombinerClass(MostFrequentDestReducer.class);
		jobTracker.setOutputKeyClass(Text.class);
		jobTracker.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobTracker, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobTracker,new Path(args[1]));
		return (jobTracker.waitForCompletion(true)?0:1);
	}
	
}
