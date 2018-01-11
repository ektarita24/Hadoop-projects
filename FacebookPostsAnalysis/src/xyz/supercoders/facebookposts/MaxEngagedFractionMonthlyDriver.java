package xyz.supercoders.facebookposts;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.supercoders.facebookposts.custom.PostDetailsWritable;

public class MaxEngagedFractionMonthlyDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Max Engaged Fraction of Post");
		job.setJarByClass(xyz.supercoders.facebookposts.MaxEngagedFractionMonthlyDriver.class);
		
		job.setMapperClass(xyz.supercoders.facebookposts.MonthPostDetailsMapper.class);
		job.setCombinerClass(MaxEngagedFractionMonthlyCombiner.class);
		job.setReducerClass(xyz.supercoders.facebookposts.MaxEngagedFractionMonthlyReducer.class);
		
		//job.setNumReduceTasks(2);
		// TODO: specify output types
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PostDetailsWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/home/bloombench/Documents/Ekta-Hadoop/hadoop-kit/data/FacebookData.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/home/bloombench/Documents/Ekta-Hadoop/HadoopProject/FacebookOutputQ1"));

		if (!job.waitForCompletion(true))
			return;
	}

}
