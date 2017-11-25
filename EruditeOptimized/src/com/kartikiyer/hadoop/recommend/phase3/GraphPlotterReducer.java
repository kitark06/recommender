package com.kartikiyer.hadoop.recommend.phase3;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.kartikiyer.hadoop.recommend.phase2.PermutationGeneratorMapper;


public class GraphPlotterReducer extends Reducer<NodePairWritable, NodePairWritable, Text, Text>
{
	Text	keyOut	= new Text();
	Text	valueOut	= new Text();

	public void reduce(NodePairWritable key, Iterable<NodePairWritable> values, Context context) throws IOException, InterruptedException
	{
		StringBuilder recommendations = new StringBuilder();
		for (NodePairWritable destination : values)
		{
			String line = destination.toString();

			String[] nodePair = line.split("\t")[0].split("\\" + PermutationGeneratorMapper.DIRECTED_NODE_DELIMITER);
			String edgeWeigth = line.split("\t")[1];

			recommendations.append("[" + nodePair[1] + "," + edgeWeigth + "] ,");
		}

		recommendations.setLength(recommendations.length() - 1);

		keyOut.set(key.getSourceNode());
		valueOut.set(recommendations.toString());

		context.write(keyOut, valueOut);

	}
}

