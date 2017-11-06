package com.kartikiyer.hadoop.recommend.phase1;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class MediaIDGrouper extends WritableComparator
{
	protected MediaIDGrouper()
	{
		super(TextCompositeWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b)
	{
		TextCompositeWritable t1 = (TextCompositeWritable) a;
		TextCompositeWritable t2 = (TextCompositeWritable) b;
		return t1.getFirst().compareTo(t2.getFirst());
	}
}