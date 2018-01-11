package xyz.supercoders.facebookposts;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.supercoders.facebookposts.custom.PostDetailsWritable;

public class MonthPostDetailsMapper extends Mapper<LongWritable, Text, IntWritable, PostDetailsWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String info[] = line.split(",");
		
		String type = info[1];
		int category = Integer.parseInt(info[2]);
		int month = Integer.parseInt(info[3]);
		double lifetimeEngagedUsers = Double.parseDouble(info[9]);
		double lifetimePostTotalReach = Double.parseDouble(info[7]);
		double peopleEngagedFraction = lifetimeEngagedUsers/lifetimePostTotalReach;
		
		PostDetailsWritable postDetailsWritable = new PostDetailsWritable(new Text(type), new IntWritable(category), new DoubleWritable(peopleEngagedFraction));
		context.write(new IntWritable(month), postDetailsWritable);
	}
}
