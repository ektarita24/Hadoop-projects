package xyz.supercoders.weatherbigdata;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import xyz.supercoders.weatherbigdata.custom.TemperatureRecordWritable;

public class YearTemperatureMapper extends Mapper<LongWritable, Text, IntWritable, TemperatureRecordWritable> {
//										   Mapper<Input Key, Input value, Output Key, Output value>
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] tokens = line.split("\\|");
		//Filtering
		if(tokens.length == 5){
			//Projection
			int year = Integer.parseInt(tokens[1]);
			float temp = Float.parseFloat(tokens[4]);
			int stationNo = Integer.parseInt(tokens[0]);
			
			//Give the output to reducer as input 
			context.write(new IntWritable(year), new TemperatureRecordWritable(new FloatWritable(temp),new IntWritable(stationNo)));
		}
	}
}
