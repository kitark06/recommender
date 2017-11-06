package com.kartikiyer.hadoop.recommend.phase3;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.kartikiyer.hadoop.recommend.phase2.PermutationGeneratorMapper;


public class NodePairWritable implements WritableComparable<NodePairWritable>
{

	private Text			sourceNode			= new Text();
	private Text			destinationNode		= new Text();
	private LongWritable	edgeWeight			= new LongWritable();

	final String			DIRECTED_NODE_DELIMITER	= "~";

	public NodePairWritable()
	{
		set(new Text(), new Text(), new LongWritable());
	}

	public NodePairWritable(Text sourceNode, Text destinationNode, LongWritable edgeWeight)
	{
		set(sourceNode, destinationNode, edgeWeight);
	}

	public NodePairWritable(String sourceNode, String destinationNode, Long edgeWeight)
	{
		set(new Text(sourceNode), new Text(destinationNode), new LongWritable(edgeWeight));
	}

	public void set(Text sourceNode, Text destinationNode, LongWritable edgeWeight)
	{
		this.sourceNode = sourceNode;
		this.destinationNode = destinationNode;
		this.edgeWeight = edgeWeight;
	}

	public void set(String sourceNode, String destinationNode, Long edgeWeight)
	{
		this.sourceNode.set(sourceNode);
		this.destinationNode.set(destinationNode);
		this.edgeWeight.set(edgeWeight);
	}

	public Text getSourceNode()
	{
		return sourceNode;
	}

	public Text getdestinationNode()
	{
		return destinationNode;
	}

	public LongWritable getEdgeWeight()
	{
		return edgeWeight;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		sourceNode.write(out);
		destinationNode.write(out);
		edgeWeight.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		sourceNode.readFields(in);
		destinationNode.readFields(in);
		edgeWeight.readFields(in);
	}

	@Override
	public int compareTo(NodePairWritable that)
	{
		int cmpSourceNode = this.sourceNode.compareTo(that.getSourceNode());

		if (cmpSourceNode != 0)
			return cmpSourceNode;
		else
		{
			int compareWeight = this.edgeWeight.compareTo(that.getEdgeWeight());
			if (compareWeight != 0)
				return compareWeight;
			else
			{
				return this.destinationNode.compareTo(that.getdestinationNode());
			}
		}
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof NodePairWritable)
		{
			NodePairWritable thatNode = (NodePairWritable) obj;
			return this.getSourceNode().equals(thatNode.getSourceNode()) && this.getdestinationNode().equals(thatNode.getdestinationNode())
						&& this.getEdgeWeight().equals(thatNode.getEdgeWeight());
		}
		else
			return false;
	}

	@Override
	public int hashCode()
	{
		return this.sourceNode.hashCode() * 163 + this.destinationNode.hashCode() + this.edgeWeight.hashCode();
	}

	@Override
	public String toString()
	{
		return this.sourceNode.toString() + PermutationGeneratorMapper.DIRECTED_NODE_DELIMITER + this.destinationNode.toString() + "\t" + this.edgeWeight.toString();
	}

}
