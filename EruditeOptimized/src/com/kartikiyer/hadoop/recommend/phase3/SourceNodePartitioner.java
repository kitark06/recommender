package com.kartikiyer.hadoop.recommend.phase3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class SourceNodePartitioner extends HashPartitioner<NodePairWritable, NodePairWritable>
{
	@Override
	public int getPartition(NodePairWritable key, NodePairWritable value, int numReduceTasks)
	{
		return key.getSourceNode().hashCode() * 163 % numReduceTasks;
	}
}
