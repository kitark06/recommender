package com.kartikiyer.hadoop.recommend.io;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.kartikiyer.hadoop.recommend.brute.TextToMapFileOperations;
import com.kartikiyer.hadoop.recommend.phase2.PermutationGeneratorMapper;
import com.kartikiyer.hadoop.recommend.phase3.NodePairWritable;

public class GraphPlotterReducer2 extends Reducer<NodePairWritable, Text, Text, Text>
{
	TextToMapFileOperations map;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		map = new TextToMapFileOperations();
	}
	
	public void reduce(NodePairWritable key , Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		StringBuilder recommendations = new StringBuilder(); 
		for (Text destination : values)
		{
			String line = destination.toString();
			
			System.out.println("-->"+destination+"<--");
			String[] nodePair = line.split("\t")[0].split("\\"+PermutationGeneratorMapper.DIRECTED_NODE_DELIMITER);
			String edgeWeigth = line.split("\t")[1];
			
			recommendations.append("[" + map.getValue(new Text(nodePair[1]))+ "," + edgeWeigth + "] ,");
		}
		
		recommendations.setLength(recommendations.length()-1);
		
		context.write(new Text(map.getValue(key.getSourceNode())), new Text(recommendations.toString()));
	}
	
	
	public static void main(String[] args)
	{
		String line = "d~a	1	d~a	1";
		
		String[] nodePair = line.split("\t")[0].split("\\"+PermutationGeneratorMapper.DIRECTED_NODE_DELIMITER);
		String edgeWeigth = line.split("\t")[1];
		
		System.out.println(edgeWeigth);
	}
}

