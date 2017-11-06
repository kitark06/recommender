package com.kartikiyer.hadoop.recommend.phase1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LikeStreamMapper extends Mapper<LongWritable, Text, TextCompositeWritable, Text>
{
	private TextCompositeWritable compositeKey = new TextCompositeWritable();
	private Text movieID = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		
		String[] data = line.split("\\t");
		
		String userID = data[0];
		String mediaID = data[1];
		
		compositeKey.set(userID,mediaID);
		
		movieID.set(mediaID);
		context.write(compositeKey, movieID);
	}
}
