package com.kartikiyer.hadoop.recommend.phase1;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class MediaIDSecondarySorter extends WritableComparator
{
	protected MediaIDSecondarySorter()
	{
		super(TextCompositeWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b)
	{
		TextCompositeWritable t1 = (TextCompositeWritable) a;
		TextCompositeWritable t2 = (TextCompositeWritable) b;
		return t1.compareTo(t2);
	}
}