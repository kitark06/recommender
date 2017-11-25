package com.kartikiyer.hadoop.recommend;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.kartikiyer.hadoop.recommend.io.TextToMapFileOperations;
import com.kartikiyer.hadoop.recommend.phase1.LikeAggregatorReducer;
import com.kartikiyer.hadoop.recommend.phase1.LikeStreamMapper;
import com.kartikiyer.hadoop.recommend.phase1.MediaIDGrouper;
import com.kartikiyer.hadoop.recommend.phase1.MediaIDSecondarySorter;
import com.kartikiyer.hadoop.recommend.phase1.TextCompositeWritable;
import com.kartikiyer.hadoop.recommend.phase1.UserIdHashPartitioner;
import com.kartikiyer.hadoop.recommend.phase2.GraphEdgeWeightReducer;
import com.kartikiyer.hadoop.recommend.phase2.PermutationGeneratorMapper;
import com.kartikiyer.hadoop.recommend.phase3.GraphEdgeSplitMapper;
import com.kartikiyer.hadoop.recommend.phase3.GraphPlotterReducer;
import com.kartikiyer.hadoop.recommend.phase3.GraphWeightDescendingSorter;
import com.kartikiyer.hadoop.recommend.phase3.NodePairWritable;
import com.kartikiyer.hadoop.recommend.phase3.SourceNodeGrouper;
import com.kartikiyer.hadoop.recommend.phase3.SourceNodePartitioner;


public class EruditeOptimized extends Configured implements Tool
{

	private static String	phaseOneInput;
	private static String	phaseOneOutput;

	private static String	phaseTwoInput;
	private static String	phaseTwoOutput;

	private static String	phaseThreeInput;
	private static String	phaseThreeOutput;

	static int			numberOfReducers	= 2;

	public ControlledJob getPhaseOneJob() throws IOException
	{
		Job job = Job.getInstance(getConf());

		job.setJobName("Phase 1 - Optimized");

		job.setJarByClass(EruditeOptimized.class);
		job.setMapperClass(LikeStreamMapper.class);
		job.setReducerClass(LikeAggregatorReducer.class);

		job.setMapOutputKeyClass(TextCompositeWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(UserIdHashPartitioner.class);
		job.setSortComparatorClass(MediaIDSecondarySorter.class);
		job.setGroupingComparatorClass(MediaIDGrouper.class);

		job.setNumReduceTasks(numberOfReducers);

		Path in = new Path(phaseOneInput);
		FileInputFormat.addInputPath(job, in);

		Path out = new Path(phaseOneOutput);
		FileOutputFormat.setOutputPath(job, out);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(out))
			fs.delete(out, true);

		ControlledJob controlledJob = new ControlledJob(job.getConfiguration());
		controlledJob.setJob(job);

		return controlledJob;
	}

	public ControlledJob getPhaseTwoJob() throws IOException
	{
		Job job = Job.getInstance(getConf());

		job.setJobName("Phase 2 - Optimized");
		job.setJarByClass(EruditeOptimized.class);
		job.setMapperClass(PermutationGeneratorMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setCombinerClass(GraphEdgeWeightReducer.class);
		job.setReducerClass(GraphEdgeWeightReducer.class);

		job.setNumReduceTasks(numberOfReducers);

		Path in = new Path(phaseTwoInput);
		FileInputFormat.addInputPath(job, in);

		Path out = new Path(phaseTwoOutput);
		FileOutputFormat.setOutputPath(job, out);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(out))
			fs.delete(out, true);

		ControlledJob controlledJob = new ControlledJob(job.getConfiguration());
		controlledJob.setJob(job);

		return controlledJob;
	}

	public ControlledJob getPhaseThreeJob() throws IOException
	{
		Job job = Job.getInstance(getConf());

		job.setJobName("Phase 3 - Optimized");
		job.setJarByClass(EruditeOptimized.class);
		job.setMapperClass(GraphEdgeSplitMapper.class);

		job.setMapOutputKeyClass(NodePairWritable.class);
		job.setMapOutputValueClass(NodePairWritable.class);

		job.setPartitionerClass(SourceNodePartitioner.class);
		job.setSortComparatorClass(GraphWeightDescendingSorter.class);
		job.setGroupingComparatorClass(SourceNodeGrouper.class);

		job.setReducerClass(GraphPlotterReducer.class);

		job.setNumReduceTasks(numberOfReducers);

		Path in = new Path(phaseThreeInput);
		FileInputFormat.addInputPath(job, in);

		Path out = new Path(phaseThreeOutput);
		FileOutputFormat.setOutputPath(job, out);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(out))
			fs.delete(out, true);

		ControlledJob controlledJob = new ControlledJob(job.getConfiguration());
		controlledJob.setJob(job);

		return controlledJob;
	}

	@Override
	public int run(String[] args) throws IOException, InterruptedException
	{
		final JobControl masterMind = new JobControl("MasterMind");

		ControlledJob phase1 = getPhaseOneJob();
		masterMind.addJob(phase1);

		ControlledJob phase2 = getPhaseTwoJob();
		phase2.addDependingJob(phase1);
		masterMind.addJob(phase2);

		ControlledJob phase3 = getPhaseThreeJob();
		phase3.addDependingJob(phase2);
		masterMind.addJob(phase3);

		new Timer().schedule(new TimerTask()
		{
			@Override
			public void run()
			{
				if (masterMind.allFinished())
				{
					masterMind.stop();
					this.cancel();
				}
			}
		}, 0, 3000);


		masterMind.run();

		if (masterMind.getFailedJobList().size() >= 1)
			printLogsToSysErrStream(masterMind);

		return 0;
	}

	private void printLogsToSysErrStream(JobControl masterMind) throws IOException, InterruptedException
	{

		StringBuilder errorMsg = new StringBuilder();

		for (ControlledJob failedControlJob : masterMind.getFailedJobList())
		{
			if (failedControlJob.getJobState() == ControlledJob.State.FAILED)
			{
				Configuration failedConf = failedControlJob.getJob().getConfiguration();
				FileSystem fs = FileSystem.get(failedConf);

				Path aggregatedErrorLog = new Path(failedConf.get("yarn.nodemanager.remote-app-log-dir") + "/" + failedConf.get("user.name") + "/logs/"
							+ failedControlJob.getJob().getJobID().toString().replaceFirst("job", "application"));

				RemoteIterator<LocatedFileStatus> errorLogs = fs.listFiles(aggregatedErrorLog, false);

				int numberOfAttempts = 3;

				while (numberOfAttempts > 0)
				{
					try (
						BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(errorLogs.next().getPath())));)
					{
						String line;
						while ((line = br.readLine()) != null)
							System.err.println(line);
						break;
					}
					catch (NoSuchElementException e)
					{
						System.err.println("Getting logs.. please wait");
						e.printStackTrace();
						Thread.sleep(20000);
						numberOfAttempts--;
					}
				}
			}
			errorMsg.append(failedControlJob.getJobName() + " :: " + failedControlJob.getJobState() + "\n");
		}

		throw new RuntimeException("\n\n\n\nThe following Jobs FAILED \n" + errorMsg.toString());


	}

	public void visualizeTheOutput(Path mapFileDir, boolean isRecreateMapFile, Path inputForMapFileCreation, ArrayList<InputStream> inputFileStream, Path outputFile)
		throws IllegalArgumentException, IOException
	{
		TextToMapFileOperations hadoopFileIO = new TextToMapFileOperations(mapFileDir);

		if (isRecreateMapFile)
			hadoopFileIO.computeAndWriteMapFile(inputForMapFileCreation);

		for (InputStream stream : inputFileStream)
			hadoopFileIO.replaceMovieIdsWithName(stream, outputFile);
	}

	public static void main(String[] args) throws Exception
	{
		System.setProperty("hadoop.home.dir", "C:\\winutils-master\\hadoop-2.7.1");
		System.setProperty("HADOOP_USER_NAME", "cloudera");

		Configuration conf = new Configuration();
		conf.set("mapreduce.job.user.name", "cloudera");
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("dfs.client.use.datanode.hostname", "true");

		phaseOneInput = "/user/" + conf.get("mapreduce.job.user.name") + "/recommender/LoadInput";
		// phaseOneInput = "/user/" + conf.get("mapreduce.job.user.name") + "/recommender/input";
		phaseOneOutput = "/user/" + conf.get("mapreduce.job.user.name") + "/recommender/output";

		phaseTwoInput = phaseOneOutput;
		phaseTwoOutput = "/user/" + conf.get("mapreduce.job.user.name") + "/recommender/outputPhase2";

		phaseThreeInput = phaseTwoOutput;
		phaseThreeOutput = "/user/" + conf.get("mapreduce.job.user.name") + "/recommender/outputPhase3";


		// Setting custom JDK version for the containers .. JDK 1.8
		String javaHome = "JAVA_HOME=/usr/java/jdk1.8.0_151";
		conf.set("mapreduce.map.env", javaHome);
		conf.set("mapreduce.reduce.env", javaHome);
		conf.set("yarn.app.mapreduce.am.env", javaHome);

		// Compression settings to optimize job
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("mapreduce.output.fileoutputformat.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");

//		ToolRunner.run(conf, new EruditeOptimized(), args);

		Path p = new Path(phaseThreeOutput);
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(p, false);

		ArrayList<InputStream> streams = new ArrayList<>();
		
		while (files.hasNext())
		{
			LocatedFileStatus locatedFile = files.next();
			if (locatedFile.getPath().toString().contains("_SUCCESS") == false)
				{
					InputStream is = fs.open(locatedFile.getPath());
					BZip2CompressorInputStream bis= new BZip2CompressorInputStream(is);
					streams.add(bis);
					System.out.println(locatedFile);
				}
		}


		new EruditeOptimized().visualizeTheOutput(new Path("/user/cloudera/MapFiles/MovieID_Name"), false, new Path("/user/cloudera/recommendeer/input/Movie.txt"), streams, new Path("/user/cloudera/recommender/VisualizeOutput.txt"));

	}
}
