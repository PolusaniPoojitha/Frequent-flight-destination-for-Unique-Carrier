package UIS.BigDataAnalytics.Assignment1;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.*;
public class MostFrequentDestReducer extends Reducer<Text,Text,Text,Text> { 
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
	{
		HashMap<Text,Integer> SourceCollection=new HashMap<Text,Integer>();
		//frequency=key.getValue();
		for(Text value:values)
		{
			Integer count=SourceCollection.get(value);
			SourceCollection.put(value, (count==null)?1:count+1);
		}
		int maxValue=Collections.max(SourceCollection.values());
		for(Map.Entry<Text,Integer> SrcInfo:SourceCollection.entrySet())
		{
			if(SrcInfo.getValue()==maxValue)
			{
				context.write(key,new Text(SrcInfo.getKey().toString()+"\t"+SrcInfo.getValue().toString()));	
			}
		}   
	}	
}
