package xyz.supercoders.weatherbigdata.custom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class TemperatureRecordWritable implements Writable {

	private FloatWritable temperature;
	
	private IntWritable stationNo;
	
	public TemperatureRecordWritable(){
		temperature = new FloatWritable();
		stationNo = new IntWritable();
	}
	
	public TemperatureRecordWritable(FloatWritable temperature, IntWritable stationNo) {
		super();
		this.temperature = temperature;
		this.stationNo = stationNo;
	}

	public FloatWritable getTemperature() {
		return temperature;
	}

	public IntWritable getStationNo() {
		return stationNo;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		stationNo.readFields(input);
		temperature.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		stationNo.write(output);
		temperature.write(output);
	}

}
