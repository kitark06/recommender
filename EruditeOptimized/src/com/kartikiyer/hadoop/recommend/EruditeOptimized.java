package com.kartikiyer.hadoop.recommend;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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

		job.setNumReduceTasks(1);

		Path in = new Path(phaseOneInput);
		FileInputFormat.addInputPath(job, in);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path out = new Path(phaseOneOutput);

		if (fs.exists(out))
			fs.delete(out, true);
		FileOutputFormat.setOutputPath(job, out);

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

		// job.setCombinerClass(GraphEdgeWeightReducer.class);
		job.setReducerClass(GraphEdgeWeightReducer.class);

		job.setNumReduceTasks(4);

		Path in = new Path(phaseTwoInput);
		FileInputFormat.addInputPath(job, in);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path out = new Path(phaseTwoOutput);
		if (fs.exists(out))
			fs.delete(out, true);
		FileOutputFormat.setOutputPath(job, out);

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
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(SourceNodePartitioner.class);
		job.setSortComparatorClass(GraphWeightDescendingSorter.class);
		job.setGroupingComparatorClass(SourceNodeGrouper.class);

		job.setReducerClass(GraphPlotterReducer.class);

		job.setNumReduceTasks(1);

		Path in = new Path(phaseThreeInput);
		FileInputFormat.addInputPath(job, in);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path out = new Path(phaseThreeOutput);
		if (fs.exists(out))
			fs.delete(out, true);
		FileOutputFormat.setOutputPath(job, out);

		ControlledJob controlledJob = new ControlledJob(job.getConfiguration());
		controlledJob.setJob(job);

		return controlledJob;
	}

	@Override
	public int run(String[] args) throws IOException
	{
		final JobControl masterMind = new JobControl("MasterMind - Optimized");

		ControlledJob phase1 = getPhaseOneJob();
		masterMind.addJob(phase1);

		ControlledJob phase2 = getPhaseTwoJob();
		phase2.addDependingJob(phase1);

		masterMind.addJob(phase2);

		/*ControlledJob phase3 = getPhaseThreeJob();
		phase3.addDependingJob(phase2);
		masterMind.addJob(phase3);*/

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

		List<ControlledJob> failedControlledJobs = masterMind.getFailedJobList();
		if (failedControlledJobs.size() >= 1)
		{
			StringBuilder errorMsg = new StringBuilder();

			for (ControlledJob failedControlJob : failedControlledJobs)
			{
				if (failedControlJob.getJobState() == ControlledJob.State.FAILED)
				{
					Configuration failedConf = failedControlJob.getJob().getConfiguration();
					FileSystem fs = FileSystem.get(failedConf);

					Path aggregatedErrorLog = new Path(failedConf.get("yarn.nodemanager.remote-app-log-dir") + "/" + failedConf.get("user.name") + "/logs/"
								+ failedControlJob.getJob().getJobID().toString().replaceFirst("job", "application"));


					RemoteIterator<LocatedFileStatus> errorLogs = fs.listFiles(aggregatedErrorLog, false);
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(errorLogs.next().getPath())));
					String line = br.readLine();
					while (line != null)
					{
						System.err.println(line);
						line = br.readLine();
					}
				}
				errorMsg.append(failedControlJob.getJobName() + " :: " + failedControlJob.getJobState() + "\n");
			}

			throw new RuntimeException("\n\n\n\nThe following Jobs FAILED \n" + errorMsg.toString());
		}

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		System.setProperty("hadoop.home.dir", "C:\\winutils-master\\hadoop-2.7.1");
		System.setProperty("HADOOP_USER_NAME", "cloudera");


		String phaseOneLoadInput = "/user/${mapreduce.job.user.name}/recommendeer/LoadInput";
		phaseOneInput = phaseOneLoadInput;
		// phaseOneInput = "/user/${mapreduce.job.user.name}/recommendeer/input";
		phaseOneOutput = "/user/${mapreduce.job.user.name}/recommendeer/output";

		phaseTwoInput = phaseOneOutput;
		phaseTwoOutput = "/user/${mapreduce.job.user.name}/recommendeer/outputPhase2";

		phaseThreeInput = phaseTwoOutput;
		phaseThreeOutput = "/user/${mapreduce.job.user.name}/recommendeer/outputPhase3";

		Configuration conf = new Configuration();

		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("dfs.client.use.datanode.hostname", "true");

		// Setting custom JDK version for the containers .. JDK 1.8
		String javaHome = "JAVA_HOME=/usr/java/jdk1.8.0_151";
		conf.set("mapreduce.map.env", javaHome);
		conf.set("mapreduce.reduce.env", javaHome);
		conf.set("yarn.app.mapreduce.am.env", javaHome);

		/*// Compression settings to optimize job
		conf.set("mapreduce.compress.map.output", "true");
		conf.set("mapreduce.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("mapreduce.output.compress", "true");
		conf.set("mapreduce.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");*/

		ToolRunner.run(conf, new EruditeOptimized(), args);
	}
}
