package com.kartikiyer.hadoop.recommend.phase2;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class GraphEdgeWeightReducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
	private LongWritable totalWeight = new LongWritable();

	@Override
	protected void reduce(Text directedNode, Iterable<LongWritable> weight, Context context) throws IOException, InterruptedException
	{
		long edgeCount = 0;
		for (LongWritable value : weight)
			edgeCount += 1;
		totalWeight.set(edgeCount);

		context.write(directedNode, totalWeight);
	}
}
