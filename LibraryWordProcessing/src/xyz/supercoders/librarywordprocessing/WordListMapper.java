package xyz.supercoders.librarywordprocessing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordListMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] tokens = line.split(" ");
		for(String word : tokens){
			context.write(new Text(word), new Text("W"));
		}
	}
}
