package xyz.supercoders.usercomments;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserCommentReducer extends Reducer<LongWritable, Text, Text, Text> {

	public void reduce(LongWritable _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String userInfo = null;
		ArrayList<String> comments = new ArrayList<>();
		
		for (Text val : values) {
			String str = val.toString();
			if(str.startsWith(UserMapper.MARKER)){
				userInfo = str.substring(str.indexOf('-') + 1);
			}
			else{
				comments.add(str.substring(str.indexOf('-') + 1));
			}
		}
		String userComments = StringUtils.join(comments, "|");
		context.write(new Text(userInfo), new Text(userComments));
	}

}
