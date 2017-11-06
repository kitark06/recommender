package com.kartikiyer.hadoop.recommend.phase1;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class LikeAggregatorReducer extends Reducer<TextCompositeWritable, Text, LongWritable, Text>
{
	LongWritable	keyOut	= new LongWritable();
	Text			value	= new Text();
	Text			current	= new Text();
	Text			previous	= new Text();

	@Override
	protected void reduce(TextCompositeWritable key, Iterable<Text> arg1, Context context) throws IOException, InterruptedException
	{
		// reseting them because we have a new key so this prevents older key's value bleeding over to the current key 
		previous.set("");
		current.set("");

		StringBuilder builder = new StringBuilder("");

		/*
		 *  input has repetitions which can cause incorrect grouping in the latest stages
		 *  Since the input comes from mapper, its already sorted and 
		 *  hence checking current value with prev and adding that to make sure 
		 *  that only distinct elements are passed through.
		 */
		for (Text text : arg1)
		{
			current.set(text);
			
			if (current.equals(previous) == false)
				builder.append(text + ",");

			previous.set(current);
		}

		builder.setLength(builder.length() - 1); // to remove the last comma

		value.set(builder.toString());
		keyOut.set(Long.parseLong(key.getFirst().toString()));

		context.write(keyOut, value);
	}
}
