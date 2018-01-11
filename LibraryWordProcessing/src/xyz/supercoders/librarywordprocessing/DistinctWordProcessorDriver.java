package xyz.supercoders.librarywordprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistinctWordProcessorDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Distinct Words List");
		job1.setJarByClass(xyz.supercoders.librarywordprocessing.DistinctWordProcessorDriver.class);
		job1.setMapperClass(xyz.supercoders.librarywordprocessing.WordListMapper.class);

		job1.setReducerClass(xyz.supercoders.librarywordprocessing.DistinctWordReducer.class);
		job1.setCombinerClass(xyz.supercoders.librarywordprocessing.DistinctWordCombiner.class);
		
		// TODO: specify output types
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job1, new Path("/home/bloombench/Documents/Ekta-Hadoop/hadoop-kit/data/word_count_input/text_data"));
		FileOutputFormat.setOutputPath(job1, new Path("/home/bloombench/hadoop/Output/DistinctWords"));

		/* Returns only single job to yarn
		 * if (!job.waitForCompletion(true))
			return;*/
		
		Job job2 = Job.getInstance(conf, "Distinct Words Count");
		job2.setJarByClass(xyz.supercoders.librarywordprocessing.DistinctWordProcessorDriver.class);
		job2.setMapperClass(xyz.supercoders.librarywordprocessing.WordCountMapperOfJob2.class);

		job2.setReducerClass(xyz.supercoders.librarywordprocessing.WordCountReducerOfJob2.class);
		
		// TODO: specify output types
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job2, new Path("/home/bloombench/hadoop/Output/DistinctWords/part-r*"));
		FileOutputFormat.setOutputPath(job2, new Path("/home/bloombench/hadoop/Output/WordCount"));
		
		ControlledJob cj1 = new ControlledJob(conf);
		cj1.setJob(job1);
		
		ControlledJob cj2 = new ControlledJob(conf);
		cj2.setJob(job2);
		
		cj2.addDependingJob(cj1);
		
		JobControl jc = new JobControl("Distinct Words from a stream");
		jc.addJob(cj1);
		jc.addJob(cj2);
		
		Thread thread = new Thread(jc);
		thread.setDaemon(true);
		thread.start();
		
		while(!jc.allFinished()){
			System.out.println("Still Running...");
			Thread.sleep(1000);
		}
		
		System.out.println("All jobs done!!");
	}
}
