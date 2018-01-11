package xyz.supercoders.usercomments;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserCommentsDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(xyz.supercoders.usercomments.UserCommentsDriver.class);
		
		job.setReducerClass(xyz.supercoders.usercomments.UserCommentReducer.class);

		// TODO: specify output types
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//joining of different mapper outputs to a common reducer
		MultipleInputs.addInputPath(job, new Path("/home/userapril17/hadoop-kit/data/comments_data/input/users.txt"), 
				TextInputFormat.class, UserMapper.class);
		
		MultipleInputs.addInputPath(job, new Path("/home/userapril17/hadoop-kit/data/comments_data/input/comments.txt"), 
				TextInputFormat.class, CommentsMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path("/home/bloombench/hadoop/Output/UserComments"));

		if (!job.waitForCompletion(true))
			return;
	}

}
