package xyz.supercoders.weatherbigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import xyz.supercoders.weatherbigdata.custom.TemperatureRecordWritable;

public class MaxTemperatureYearlyDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(xyz.supercoders.weatherbigdata.MaxTemperatureYearlyDriver.class);
		job.setMapperClass(xyz.supercoders.weatherbigdata.YearTemperatureMapper.class);

		job.setReducerClass(xyz.supercoders.weatherbigdata.MaxTemperatureYearlyReducer.class);
		//job.setCombinerClass(xyz.supercoders.weatherbigdata.MaxTemperatureYearlyReducer.class);
		//job.setNumReduceTasks(2);
		
		//Set Map and reducer output key and value
		job.setMapOutputKeyClass(IntWritable.class);	//Output key of map
		job.setMapOutputValueClass(TemperatureRecordWritable.class);//Output key of map
		job.setOutputKeyClass(IntWritable.class);  		//Output key of reducer
		job.setOutputValueClass(Text.class);	//Output value of reducer

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/ekta/weather-input-data"));
		FileOutputFormat.setOutputPath(job, new Path("/ekta/weather-output-data"));

		if (!job.waitForCompletion(true))
			return;
	}

}
