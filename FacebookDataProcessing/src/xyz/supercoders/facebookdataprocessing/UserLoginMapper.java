package xyz.supercoders.facebookdataprocessing;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserLoginMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] tokens = line.split(",");
		
		if(tokens[3].equalsIgnoreCase("in")){
			context.write(new Text(tokens[0]), new IntWritable(1));
		}
	}

}
