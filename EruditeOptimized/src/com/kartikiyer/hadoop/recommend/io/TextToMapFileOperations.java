package com.kartikiyer.hadoop.recommend.io;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

import com.kartikiyer.hadoop.recommend.phase2.PermutationGeneratorMapper;


public class TextToMapFileOperations
{
	private FileSystem		fs;
	private Path			mapFileDir;
	private Configuration	conf;

	public TextToMapFileOperations() throws IOException
	{
		System.setProperty("hadoop.home.dir", "C:\\winutils-master\\hadoop-2.7.1");
		System.setProperty("HADOOP_USER_NAME", "cloudera");

		conf = new Configuration();
		fs = FileSystem.get(conf);

		mapFileDir = new Path("/user/cloudera/MapFiles/MovieID_Name");
	}

	public void computeAndWriteMapFile() throws IOException
	{
		if(fs.exists(mapFileDir))
			fs.delete(mapFileDir,true);

		// MapFile.Writer.Option extends SequenceFile.Writer.Option ... so can be given to MapFile.Writer
		MapFile.Writer.Option keyClass = MapFile.Writer.keyClass(LongWritable.class);
		SequenceFile.Writer.Option valueClass = MapFile.Writer.valueClass(Text.class);

		try (
			MapFile.Writer mapWriter = new MapFile.Writer(conf, mapFileDir, keyClass, valueClass))
		{
			Path movieMetaData = new Path("/user/cloudera/recommendeer/input/Movie.txt");
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(movieMetaData)));
			String line = br.readLine();

			while (line != null)
			{
				LongWritable key = new LongWritable(Long.parseLong(line.split("\t")[0]));
				Text val = new Text(line.split("\t")[1]);
				mapWriter.append(key, val);
				line = br.readLine();
			}
		}
	}

	public String getValue(LongWritable movieId) throws IOException
	{
		SequenceFile.Reader.Option keyClass = MapFile.Reader.comparator(WritableComparator.get(LongWritable.class, conf));
		try (
			MapFile.Reader mapReader = new MapFile.Reader(mapFileDir, conf, keyClass);)
		{
			Text value = new Text();
			mapReader.get(movieId, value);
			return value.toString();
		}
	}

	public String getValue(Text movieId) throws IOException
	{
		return getValue(new LongWritable(Long.parseLong(movieId.toString())));
	}

	public static void main(String[] args) throws IOException, InterruptedException
	{
		new TextToMapFileOperations().computeAndWriteMapFile();
		System.out.println(new TextToMapFileOperations().getValue(new LongWritable(765L)));
	}
	
	private void replaceMovieIdsWithName(String file)
	{
		Path inputFile = new Path(file);
		
		/*inputFile.
		
		
		StringBuilder recommendations = new StringBuilder(); 
		for (Text destination : values)
		{
			String line = destination.toString();
			
			System.out.println("-->"+destination+"<--");
			String[] nodePair = line.split("\t")[0].split("\\"+PermutationGeneratorMapper.DIRECTED_NODE_DELIMITER);
			String edgeWeigth = line.split("\t")[1];
			
			recommendations.append("[" + map.getValue(new Text(nodePair[1]))+ "," + edgeWeigth + "] ,");
		}
		
		recommendations.setLength(recommendations.length()-1);
		
		context.write(new Text(map.getValue(key.getSourceNode())), new Text(recommendations.toString()));*/
	}
	
}
