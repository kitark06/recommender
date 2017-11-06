package com.kartikiyer.hadoop.recommend.phase3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class SourceNodeGrouper extends WritableComparator
{

	public SourceNodeGrouper()
	{
		super(NodePairWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b)
	{
		Text sourceNode1 = ((NodePairWritable) a).getSourceNode();
		Text sourceNode2 = ((NodePairWritable) b).getSourceNode();
		return sourceNode1.compareTo(sourceNode2);
	}
}
