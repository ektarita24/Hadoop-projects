package xyz.supercoders.facebookdataprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserLoginCountDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Facebook Login Count");
		job.setJarByClass(xyz.supercoders.facebookdataprocessing.UserLoginCountDriver.class);
		job.setMapperClass(xyz.supercoders.facebookdataprocessing.UserLoginMapper.class);

		job.setReducerClass(xyz.supercoders.facebookdataprocessing.UserLoginCountReducer.class);

		// TODO: specify output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/home/bloombench/Documents/Ekta-Hadoop/hadoop-kit/data/facebook_logs"));
		FileOutputFormat.setOutputPath(job, new Path("/home/bloombench/hadoop/Output/FacebookData"));

		if (!job.waitForCompletion(true))
			return;
	}

}
