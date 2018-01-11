package com.supercoders.facebookposts.custom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PostDetailsWritable implements Writable {

	private Text type;
	private IntWritable category;
	private DoubleWritable peopleEngagedFraction;
	
	public PostDetailsWritable(){
		type = new Text();
		category = new IntWritable();
		peopleEngagedFraction = new DoubleWritable();
	}
	
	public PostDetailsWritable(Text type, IntWritable category, DoubleWritable peopleEngagedFraction) {
		super();
		this.type = type;
		this.category = category;
		this.peopleEngagedFraction = peopleEngagedFraction;
	}

	public Text getType() {
		return type;
	}

	public IntWritable getCategory() {
		return category;
	}

	public DoubleWritable getPeopleEngagedFraction() {
		return peopleEngagedFraction;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		type.readFields(input);
		category.readFields(input);
		peopleEngagedFraction.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		type.write(output);
		category.write(output);
		peopleEngagedFraction.write(output);
	}

}
