package com.kartikiyer.hadoop.recommend.phase2;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PermutationGeneratorMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	public final static String	DIRECTED_NODE_DELIMITER	= "~";
	LongWritable				ONE					= new LongWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{

		String[] values = value.toString().split("\\t")[1].split(",");

		ArrayList<String> nodes = new ArrayList<>();

		for (int i = 0; i < values.length; i++)
			for (int j = i + 1 ; j < values.length; j++)
				nodes.add(values[i] + DIRECTED_NODE_DELIMITER + values[j]);

		for (String directedNode : nodes)
			context.write(new Text(directedNode), ONE);
	}

}
