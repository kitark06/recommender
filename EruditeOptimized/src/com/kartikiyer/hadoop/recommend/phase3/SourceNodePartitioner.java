package com.kartikiyer.hadoop.recommend.phase3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class SourceNodePartitioner extends HashPartitioner<NodePairWritable, Text>
{
	@Override
	public int getPartition(NodePairWritable key, Text value, int numReduceTasks)
	{
		return key.getSourceNode().hashCode() * 163 % numReduceTasks;
	}
}
