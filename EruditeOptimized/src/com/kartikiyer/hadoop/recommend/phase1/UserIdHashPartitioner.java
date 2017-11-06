package com.kartikiyer.hadoop.recommend.phase1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class UserIdHashPartitioner extends HashPartitioner<TextCompositeWritable, Text>
{
	@Override
	public int getPartition(TextCompositeWritable compositeKey, Text value, int numReduceTasks)
	{
		return (compositeKey.getFirst().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}