package com.kartikiyer.hadoop.recommend.phase3;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class GraphWeightDescendingSorter extends WritableComparator
{
	protected GraphWeightDescendingSorter()
	{
		super(NodePairWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b)
	{
		NodePairWritable t1 = (NodePairWritable) a;
		NodePairWritable t2 = (NodePairWritable) b;
		return -t1.compareTo(t2);
	}
}