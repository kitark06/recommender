package com.kartikiyer.hadoop.recommend.io;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;


public class TextToMapFileOperations
{
	private FileSystem		fs;
	private Path			mapFileDir;
	private Configuration	conf;
	private LongWritable	key = new LongWritable();

	public TextToMapFileOperations(Path mapFileDir) throws IOException
	{
		System.setProperty("hadoop.home.dir", "C:\\winutils-master\\hadoop-2.7.1");
		System.setProperty("HADOOP_USER_NAME", "cloudera");

		conf = new Configuration();
		fs = FileSystem.get(conf);

		this.mapFileDir = mapFileDir;
	}


	public void computeAndWriteMapFile(Path inputForMapFileCreation) throws IOException
	{
		if (fs.exists(mapFileDir))
			fs.delete(mapFileDir, true);

		// MapFile.Writer.Option extends SequenceFile.Writer.Option ... so can be given to MapFile.Writer
		MapFile.Writer.Option keyClass = MapFile.Writer.keyClass(LongWritable.class);
		SequenceFile.Writer.Option valueClass = MapFile.Writer.valueClass(Text.class);

		try (
			MapFile.Writer mapWriter = new MapFile.Writer(conf, mapFileDir, keyClass, valueClass))
		{
			Path movieMetaData = inputForMapFileCreation;
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

	public String getValue(String movieId) throws IOException
	{
		key.set(Long.parseLong(movieId));
		return getValue(key);
	}

	public void replaceMovieIdsWithName(InputStream inputFileStream, Path outputFile) throws IOException
	{
		// files can be compressed , so better the caller decide and decompress the file if needed and provide an InputStream rather than hardcoding that in this method
		// InputStream stream = fs.open(inputFile);
		// BZip2CompressorInputStream stream = new BZip2CompressorInputStream(fs.open(inputFile));
		BufferedReader br = new BufferedReader(new InputStreamReader(inputFileStream));

		BufferedWriter bw;

		if (fs.exists(outputFile) == false)
			bw = new BufferedWriter(new OutputStreamWriter(fs.create(outputFile)));
		else
			bw = new BufferedWriter(new OutputStreamWriter(fs.append(outputFile)));

		// grabs and returns the contentID in the following pattern "[contentID,numericFrequency]" and replaces it with the movie/contentName in one go ..
		String regex = "(?<=\\[)[^,\\]]+";
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

		LongWritable key = new LongWritable();

		String line;
		while ((line = br.readLine()) != null)
		{
			String source = line.split("\t")[0];
			String recommendations = line.split("\t")[1];

			Matcher matcher = pattern.matcher(recommendations);

			StringBuffer sb = new StringBuffer();
			while (matcher.find())
				matcher.appendReplacement(sb, getValue(matcher.group(0)));

			matcher.appendTail(sb);

			bw.write(getValue(source) + " --> " + sb + "\n");
			bw.flush();
		}
		bw.close();
	}
}
