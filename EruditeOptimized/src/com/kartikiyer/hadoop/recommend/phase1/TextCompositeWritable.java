package com.kartikiyer.hadoop.recommend.phase1;


import java.io.*;
import org.apache.hadoop.io.*;


public class TextCompositeWritable implements WritableComparable<TextCompositeWritable>
{
	private Text	first	= new Text();
	private Text	second	= new Text();

	public TextCompositeWritable()
	{
		set(new Text(), new Text());
	}

	public TextCompositeWritable(String first, String second)
	{
		set(new Text(first), new Text(second));
	}

	public TextCompositeWritable(Text first, Text second)
	{
		set(first, second);
	}

	public void set(Text first, Text second)
	{
		this.first = first;
		this.second = second;
	}

	public void set(String first, String second)
	{
		this.first.set(first);
		this.second.set(second);
	}

	public Text getFirst()
	{
		return first;
	}

	public Text getSecond()
	{
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int hashCode()
	{
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o)
	{
		if (o instanceof TextCompositeWritable)
		{
			TextCompositeWritable tp = (TextCompositeWritable) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString()
	{
		return first + "\t" + second;
	}

	@Override
	public int compareTo(TextCompositeWritable tp)
	{
		int cmp = first.compareTo(tp.first);
		if (cmp != 0)
		{
			return cmp;
		}
		return second.compareTo(tp.second);
	}
}