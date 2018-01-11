package xyz.supercoders.weatherbigdata;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import xyz.supercoders.weatherbigdata.custom.TemperatureRecordWritable;

public class MaxTemperatureYearlyReducer extends Reducer<IntWritable, TemperatureRecordWritable, IntWritable, Text> {
//												 Reducer<InputKey, InputValue, OutputKey, OutputValue>
	public void reduce(IntWritable _key, Iterable<TemperatureRecordWritable> values, Context context) throws IOException, InterruptedException {
		
		boolean firstElementSeen = false;
		float largest = 0;
		HashSet<Integer> stationNos = new HashSet<>();
		// process values
		for (TemperatureRecordWritable val : values) {
			if(!firstElementSeen){
				largest = val.getTemperature().get();
				stationNos.add(val.getStationNo().get());
				firstElementSeen = true;
			}
			else{
				if(val.getTemperature().get()>largest){
					largest = val.getTemperature().get();
					stationNos.clear();
					stationNos.add(val.getStationNo().get());
				}
				else if(val.getTemperature().get() == largest){
					stationNos.add(val.getStationNo().get());
				}
			}
		}
		String value = largest+"|"+StringUtils.join(stationNos, ",");
		context.write(_key, new Text(value));
	}
}
