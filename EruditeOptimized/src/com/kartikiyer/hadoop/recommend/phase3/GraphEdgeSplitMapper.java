package com.kartikiyer.hadoop.recommend.phase3;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kartikiyer.hadoop.recommend.phase2.PermutationGeneratorMapper;


public class GraphEdgeSplitMapper extends Mapper<LongWritable, Text, NodePairWritable, Text>
{
	NodePairWritable compositeKey = new NodePairWritable();
				
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		// a~b 2
		String line = value.toString();

		String directedNode = line.split("\t")[0];
		String sourceNode = directedNode.split("\\"+PermutationGeneratorMapper.DIRECTED_NODE_DELIMITER)[0];
		String destinationNode = directedNode.split("\\"+PermutationGeneratorMapper.DIRECTED_NODE_DELIMITER)[1];

		Long weight = Long.parseLong(line.split("\t")[1]);

		compositeKey.set(sourceNode,destinationNode,weight);
		context.write(compositeKey, value);
		
		compositeKey.set(destinationNode,sourceNode,weight);
		context.write(compositeKey, value);
	}
}
