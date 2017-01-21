package UIS.BigDataAnalytics.Assignment1;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;

public class MostFrequentDestMapper extends Mapper<LongWritable,Text,Text,Text> {
	//MostFrequentDestComparable obj=new MostFrequentDestComparable();
	//NullWritable nullValue=NullWritable.get();
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
	
		//Columns 9,17,18
		String []columns=value.toString().split(",");
		String UniqueCarrier=columns[8];
		String Origin=columns[16];
		String Destination=columns[17];
		if(Destination!="NA" && UniqueCarrier.trim()!="UniqueCarrier")
		{
			StringBuilder buildKey=new StringBuilder();
			buildKey.append(UniqueCarrier);
			buildKey.append(" ");
			buildKey.append(Origin);
			buildKey.append(" ");
//			buildKey.append(Destination);
//			buildKey.append(" ");
			//obj.setKey(new Text(buildKey.toString()));
			//obj.setValue(new IntWritable(1));	
			//SPA-CCHg --> 1
			context.write(new Text(buildKey.toString()),new Text(Destination));
		}
	  }
}
