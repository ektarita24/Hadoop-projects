package xyz.supercoders.usercomments;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommentsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	public static final String MARKER = "UC";
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String splitText[] = ivalue.toString().split(",");
		if(splitText.length == 4){
			long id = Long.parseLong(splitText[0]);
			String comment = splitText[1];
			context.write(new LongWritable(id),new Text(MARKER + "-" + comment));
		}
	}

}
